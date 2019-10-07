package io.qross.pql

import io.qross.core.{DataRow, DataType}
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns._

object FUNCTION {
    def parse(sentence: String, PQL: PQL): Unit = {
        $USER$FUNCTION.findFirstMatchIn(sentence) match {
            case Some(m) =>
                val $function: Statement = new Statement("FUNCTION", sentence, new FUNCTION(m.group(1).trim().toUpperCase(), m.group(2).trim()))
                //只进栈
                PQL.PARSING.push($function)
                //待关闭的控制语句
                PQL.TO_BE_CLOSE.push($function)
                //继续解析第一条子语句
                PQL.parseStatement(sentence.takeAfter(m.group(0)).trim())
            case None => throw new SQLParseException("Incorrect FUNCTION sentence: " + sentence)
        }
    }
}

class FUNCTION(val functionName: String, val functionArguments: String) {

    val arguments: DataRow = new DataRow()
    functionArguments.split(",")
                    .foreach(argument => {
                        val sections = argument.trim().split(BLANKS)
                        if (!sections.head.startsWith("$")) {
                            throw new SQLParseException("Function argument must starts with '$'. " + argument)
                        }
                        val name = sections(0).substring(1)
                        val dataType = {
                            if (sections.length == 2 || sections.length > 3) {
                                DataType.ofTypeName(sections(1))
                            }
                            else {
                                DataType.AUTO
                            }
                        }
                        val defaultValue = {
                            if (sections.length > 2) {
                                if (!sections(2).equalsIgnoreCase("DEFAULT")) {
                                    throw new SQLParseException(s"Can't recognize word ${sections(2)}, it must be 'DEFAULT'. ")
                                }
                                sections.last
                            }
                            else {
                                null
                            }
                        }

                        //1 variable only
                        //2 variable and dataType
                        //3 variable and default value
                        //4 variable and dataType and default value

                        arguments.set(name, defaultValue, dataType)
                    })

    //这个方法无意义
    def execute(PQL: PQL, statement: Statement): Unit = {
        //if (PQL.dh.debugging) {
            //Output.writeDebugging("This is a placeholder of FUNCTION. Please ignore it.")
        //}
    }
}