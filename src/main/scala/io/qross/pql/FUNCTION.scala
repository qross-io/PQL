package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns._
import io.qross.pql.Solver._

object FUNCTION {

    def testArgs(args: String): String = {
        val sb = new StringBuilder()
        args.pickChars()
            .split(",")
            .map(_.trim())
            .foreach(arg => {
                if (!"""(?i)^\$[a-z0-9_]+$""".r.test(arg) && !"""(?i)^\$[a-z0-9_]+\s+DEFAULT\s+\S+[\s\S]*$""".r.test(arg)) {
                    sb.append(arg + "; ")
                }
            })

        if (sb.nonEmpty) {
            "Function arguments format is incorrect: " + sb.mkString
        }
        else {
            ""
        }
    }

    def parse(sentence: String, PQL: PQL): Unit = {
        $USER$DEFINED$FUNCTION.findFirstMatchIn(sentence) match {
            case Some(m) =>
                //检查函数参数格式是否正确
                val info = testArgs(m.group(3))
                if (info == "") {
                    val $function: Statement = new Statement("FUNCTION", sentence, new FUNCTION(m.group(1), m.group(2).trim().toUpperCase()))
                    //处理函数参数
                    m.group(3).trim().split(",")
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

                            $function.setVariable(name, if (defaultValue == null) null else defaultValue.$eval(PQL))
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
                }
                else {
                    throw new SQLParseException(info)
                }
            case None => throw new SQLParseException("Incorrect FUNCTION sentence: " + sentence)
        }
    }
}

class FUNCTION(val prefix: String, val functionName: String) {
    //, val functionArgs: String, val functionStatement: String
    //prefix 前缘是 $ 还是 @
    if (prefix == "@") {
        throw new SQLParseException("Only user function can be defined. @" + functionName)
    }

    //这个方法无意义, 函数的执行在CALL语句中
    def execute(PQL: PQL, statement: Statement): Unit = {
        //Output.writeDebugging("This is a placeholder of FUNCTION. Please ignore it.")
    }
}