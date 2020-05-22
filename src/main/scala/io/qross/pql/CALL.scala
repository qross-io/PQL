package io.qross.pql

import io.qross.exception.{SQLExecuteException, SQLParseException}
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.$CALL
import io.qross.pql.Solver._

//CALL $FUNC_NAME(2, $b := 1);

object CALL {
    def parse(sentence: String, PQL: PQL): Unit = {
        if($CALL.test(sentence)) {
            val $func = sentence.takeAfter($CALL).trim()
            if ($func.startsWith("$") || $func.startsWith("@")) {
                if ($func.contains("(") && $func.contains(")")) {
                    val funcName = $func.takeBefore("(").trim().toUpperCase()
                    val funcArgs = $func.takeBetween("(", ")")
                    PQL.PARSING.head.addStatement(new Statement("CALL", sentence, new CALL(funcName, funcArgs)))
                }
                else {
                    throw new SQLParseException("Wrong function format at CALL. " + $func)
                }
            }
            else {
                throw new SQLParseException("Only user or global function can be called. " + $func)
            }
        }
        else {
            throw new SQLParseException("Incorrect CALL sentence: " + sentence)
        }
    }
}

class CALL(funcName: String, funcArgs: String) {

    def execute(PQL: PQL, statement: Statement): Unit = {
        val symbol = funcName.take(1)
        val name = funcName.substring(1)

        if (symbol == "$" && PQL.USER$FUNCTIONS.contains(name)) {
            val $func = PQL.USER$FUNCTIONS(name)
            val $args = funcArgs.split(",").map(_.trim())

            //初始化变量
            statement.variables.combine($func.variables)

            //根据传入值赋值变量
            for (i <- $args.indices) {
                val arg = $args(i)
                if (arg.contains(":=")) {
                    val name = arg.takeBefore(":=").trim()
                    val value = arg.takeAfter(":=").trim()
                    if (name.startsWith("$")) {
                        statement.setVariable(name.takeAfter("$"), value.$eval(PQL))
                    }
                    else {
                        throw new SQLExecuteException("Wrong function argument name: " + name + " when call function " + funcName + ", variable name must starts with symbol '$'")
                    }
                }
                else {
                    $func.variables.getFieldName(i) match {
                        case Some(fieldName) => statement.setVariable(fieldName, arg.$eval(PQL))
                        case None => throw new SQLExecuteException("Out of function arguments index bound when call function. " + funcName)
                    }
                }
            }

            //执行
            PQL.EXECUTING.push(statement)
            PQL.executeStatements($func.statements)
        }
        else if (symbol == "@") {
            //system function
        }
        else {
            throw new SQLExecuteException("Wrong function name: " + funcName)
        }
    }
}
