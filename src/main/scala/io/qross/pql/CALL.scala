package io.qross.pql

import io.qross.core.DataCell
import io.qross.exception.SQLExecuteException
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.$CALL
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
                PQL.dh.stashSources() //暂存主过程中的当前数据源和目标数据源
                PQL.executeStatements($func.statements)
                PQL.dh.popSources() //恢复父级过程使用的数据源，此步骤防止函数中的语句影响主过程

                PQL.FUNCTION$RETURNS.pop()
            }
            else {
                throw new SQLExecuteException("Incorrect function name: " + funcName)
            }
        }
        else {
            //全局函数
            if (GlobalFunction.NAMES.contains(funcName)) {
                if (GlobalFunction.USER.contains(funcName)) {
                    GlobalFunction.USER(funcName).call(args.$restore(PQL))
                }
                else if (GlobalFunction.SYSTEM.contains(funcName)) {
                    GlobalFunction.SYSTEM(funcName).call(args.$restore(PQL))
                }
                else {
                    if (args == "") {
                        GlobalFunction.call(funcName, List[DataCell]())
                    }
                    else {
                        GlobalFunction.call(funcName, args.split(",").map(_.trim().$sharp(PQL)).toList)
                    }
                }
            }
            else {
                throw new SQLExecuteException("Incorrect global function name: " + funcName)
            }
        }
    }
}

class CALL(val sentence: String) {
    def execute(PQL: PQL): Unit = {
        PQL.WORKING += sentence.$eval(PQL).value
    }
}
