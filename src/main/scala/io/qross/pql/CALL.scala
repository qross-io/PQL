package io.qross.pql

import io.qross.core.DataCell
import io.qross.exception.{SQLExecuteException, SQLParseException}
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.{$CALL, FUNCTION_NAMES}
import io.qross.pql.Solver._

//CALL $FUNC_NAME(2, $b := 1);

object CALL {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("CALL", sentence, new CALL(sentence.takeAfterX($CALL))))
    }

    def call(PQL: PQL, symbol: String, funcName: String, args: String): DataCell = {
        //statement用来保存函数中的局部变量
        val statement = new Statement("CALL", "")

        if (symbol == "$") {
            //用户函数
            if (PQL.USER$FUNCTIONS.contains(funcName)) {
                val $func = PQL.USER$FUNCTIONS(funcName)
                val $args = args.split(",").map(_.trim())

                //初始化变量
                statement.variables.combine($func.variables)

                //根据传入值赋值变量
                for (i <- $args.indices) {
                    val arg = $args(i)
                    if (arg.contains(":=")) {
                        val name = arg.takeBefore(":=").trim()
                        val value = arg.takeAfter(":=").trim()
                        if (name.startsWith("$")) {
                            statement.setVariable(name.takeAfter("$"), value.$sharp(PQL))
                        }
                        else {
                            throw new SQLExecuteException("Wrong function argument name: " + name + " when call function " + name + ", variable name must starts with symbol '$'")
                        }
                    }
                    else {
                        $func.variables.getFieldName(i) match {
                            case Some(fieldName) => statement.setVariable(fieldName, arg.$eval(PQL))
                            case None => throw new SQLExecuteException("Out of function arguments index bound when call function: $" + funcName)
                        }
                    }
                }

                //执行 - 在END或RETURN语句中退出
                PQL.EXECUTING.push(statement)
                PQL.executeStatements($func.statements)

                PQL.FUNCTION$RETURNS.pop()
            }
            else {
                throw new SQLExecuteException("Incorrect function name: " + funcName)
            }
        }
        else {
            //系统函数
            //全局函数
            if (FUNCTION_NAMES.contains(funcName)) {
                new GlobalFunction(funcName).call(args.split(",").map(_.trim().$sharp(PQL)).toList)
            }
            else {
                DataCell.NULL
            }
        }
    }
}

class CALL(sentence: String) {
    def execute(PQL: PQL): Unit = {
        PQL.WORKING += sentence.$eval(PQL).value
    }
}
