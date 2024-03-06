package cn.qross.pql

import cn.qross.core.DataCell
import cn.qross.exception.SQLParseException
import cn.qross.ext.TypeExt._
import cn.qross.jdbc.{DataSource, JDBC}
import cn.qross.pql.Patterns.$END

object END {

    def parse(sentence: String, PQL: PQL): Unit = {
        sentence.takeAfterX($END).trim().toUpperCase() match {
            case "IF" =>
              //检查IF语句是否正常闭合
              if (PQL.TO_BE_CLOSE.isEmpty) {
                  throw new SQLParseException("Can't find IF clause: " + sentence)
              }
              else if (PQL.TO_BE_CLOSE.head.caption != "IF") {
                  throw new SQLParseException(PQL.TO_BE_CLOSE.head.caption + " hasn't closed: " + PQL.TO_BE_CLOSE.head.sentence)
              }
              else {
                  PQL.TO_BE_CLOSE.pop()
              }

              val $endIf: Statement = new Statement("END$IF", "END IF", new END$IF())
              //只出栈
              PQL.PARSING.pop()
              PQL.PARSING.head.addStatement($endIf)
            case "LOOP" =>
                //检查FOR语句是否正常闭合
                if (PQL.TO_BE_CLOSE.isEmpty) {
                    throw new SQLParseException("Can't find FOR or WHILE clause: " + sentence)
                }
                else if (!Set("FOR" , "WHILE").contains(PQL.TO_BE_CLOSE.head.caption)) {
                    throw new SQLParseException(PQL.TO_BE_CLOSE.head.caption + " hasn't closed: " + PQL.TO_BE_CLOSE.head.sentence)
                }
                else {
                    PQL.TO_BE_CLOSE.pop()
                }

                val $endLoop: Statement = new Statement("END$LOOP", "END LOOP", new END$LOOP())
                //只出栈
                PQL.PARSING.pop()
                PQL.PARSING.head.addStatement($endLoop)
            case "CASE" =>
                //检查CASE WHEN语句是否正常闭合
                if (PQL.TO_BE_CLOSE.isEmpty) {
                    throw new SQLParseException("Can't find CASE clause: " + sentence)
                }
                else if (PQL.TO_BE_CLOSE.head.caption != "CASE") {
                    throw new SQLParseException(PQL.TO_BE_CLOSE.head.caption + " hasn't closed: " + PQL.TO_BE_CLOSE.head.sentence)
                }
                else {
                    PQL.TO_BE_CLOSE.pop()
                }

                val $endCase: Statement = new Statement("END$CASE", "END CASE", new END$CASE())
                //只出栈
                PQL.PARSING.pop() //退出WHEN或ELSE
                PQL.PARSING.pop() //退出CASE
                PQL.PARSING.head.addStatement($endCase)
            case "" =>
                //END FUNCTION
                if (PQL.TO_BE_CLOSE.isEmpty) {
                    throw new SQLParseException("Can't find FUNCTION clause: " + sentence)
                }
                else if (PQL.TO_BE_CLOSE.head.caption != "FUNCTION") {
                    throw new SQLParseException(PQL.TO_BE_CLOSE.head.caption + " hasn't closed: " + PQL.TO_BE_CLOSE.head.sentence)
                }
                else {
                    PQL.TO_BE_CLOSE.pop()
                }

                val $end: Statement = new Statement("END", "END", new END())
                //END是FUNCTION的最后一条子语句
                PQL.PARSING.head.addStatement($end)

                //将语句从当前节点转移到functions
                val $function = PQL.PARSING.pop()
                val instance = $function.instance.asInstanceOf[FUNCTION]
                PQL.USER$FUNCTIONS += instance.functionName -> new UserFunction($function)

//                if (instance.prefix == "$") {
//
//                }
//                else if (instance.prefix == "@") {
//                    val user = PQL.credential.getInt("userid")
//                    val functionArgs = $function.variables.join(",", " DEFAULT ")
//                    val functionStatement = PQL.originalSQL.takeAfter("FUNC").takeAfter("BEGIN").takeBefore("END")
//                    if (user == 0) {
//                        GlobalFunction.SYSTEM += instance.functionName -> new GlobalFunction(instance.functionName, functionArgs, functionStatement)
//                    }
//                    else {
//                        GlobalFunction.USER += instance.functionName -> new GlobalFunction(instance.functionName, functionArgs, functionStatement)
//                    }
//
//                    // 保存到数据库
//                    if (JDBC.hasQrossSystem) {
//                        DataSource.QROSS.queryUpdate("REPLACE INTO qross_functions (function_name, function_args, function_statement, function_owner) VALUES (?, ?, ?, ?)",
//                            instance.functionName, functionArgs, functionStatement, user)
//                    }
                //}
        }
    }
}

class END {
    def execute(PQL: PQL): Unit = {
        //退出CALL FUNCTION
        PQL.FUNCTION$RETURNS += DataCell.NULL
        PQL.EXECUTING.pop()
    }
}
