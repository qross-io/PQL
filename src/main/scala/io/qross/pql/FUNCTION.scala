package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns._

object FUNCTION {
    def parse(sentence: String, PQL: PQL): Unit = {
        $USER$FUNCTION.findFirstMatchIn(sentence) match {
            case Some(m) =>
                val $function: Statement = new Statement("FUNCTION", sentence, new FUNCTION(m.group(1).trim().toUpperCase()))

                //处理函数参数
                m.group(2).trim().split(",")
                    .foreach(argument => {
                        val sections = argument.trim().split(BLANKS)
                        if (!sections.head.startsWith("$")) {
                            throw new SQLParseException("Function argument must starts with '$'. " + argument)
                        }
                        val name = sections(0).substring(1)

                        val defaultValue = {
                            if (sections.length > 2) {
                                if (!sections(1).equalsIgnoreCase("DEFAULT")) {
                                    throw new SQLParseException(s"Can't recognize word ${sections(1)}, it must be 'DEFAULT'. ")
                                }
                                sections.last
                            }
                            else {
                                null
                            }
                        }

                        $function.setVariable(name, defaultValue)
                    })

                //只进栈
                PQL.PARSING.push($function)
                //待关闭的控制语句
                PQL.TO_BE_CLOSE.push($function)
                //继续解析第一条子语句
                val first = sentence.takeAfter(m.group(0)).trim()
                if (first != "") {
                    PQL.parseStatement(first)
                }
            case None => throw new SQLParseException("Incorrect FUNCTION sentence: " + sentence)
        }
    }
}

class FUNCTION(val functionName: String) {

    //这个方法无意义, 函数的执行在CALL语句中
    def execute(PQL: PQL, statement: Statement): Unit = {
        //if (PQL.dh.debugging) {
            //Output.writeDebugging("This is a placeholder of FUNCTION. Please ignore it.")
        //}
    }
}