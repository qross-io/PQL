package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.pql.Patterns.$INVOKE
import io.qross.ext.TypeExt._
import io.qross.pql.Solver._
import io.qross.pql.Patterns.$BLANK

//INVOKE io.qross.setting.Configurations.set("key", Object "value");
//INVOKE io.qross.setting.Configurations.set("VOYAGER_LANGUAGE", chinese);
//实例类和方法暂不支持

object INVOKE {
    def parse(sentence: String, PQL: PQL): Unit = {
        $INVOKE.findFirstIn(sentence) match {
            case Some(invoke) => PQL.PARSING.head.addStatement(new Statement("INVOKE", sentence, new INVOKE(sentence.takeAfter(invoke).trim())))
            case None => throw new SQLParseException("Incorrect INVOKE sentence: " + sentence)
        }
    }
}

class INVOKE(sentence: String) {
    def execute(PQL: PQL): Unit = {
        var java = sentence

        val instance = java.startsWith("new")
        if (instance) {
            java = java.substring(3).trim()
        }

//        """\s+[,\)\(]\s+|\s+[,\)\(]|[,\(\)]\s+""".r
//            .findAllIn(sentence)
//            .foreach(symbol => {
//                java = sentence.replace(symbol, symbol.trim())
//            })

        //val java = sentence.replaceAll("\\s", "").$restore(PQL)

        val args = java.takeAfter("(")
                        .takeBefore(")")
                        .split(",")
                        .map(_.trim)
                        .map(item => {
                            var arg = item.$restore(PQL)
                            var dataType = ""
                            $BLANK.findFirstIn(arg) match {
                                case Some(blank) =>
                                    dataType = arg.takeBefore(blank).trim()
                                    arg = arg.takeAfter(blank).trim()
                                case None =>
                            }

                            if (dataType != "") {
                                dataType.toLowerCase() match {
                                    case "string" => (arg.removeQuotes(), Class.forName("java.lang.String"))
                                    case "int" | "integer" => (arg.toInt, Class.forName("java.lang.Integer"))
                                    case "object" => (arg.removeQuotes(), Class.forName("java.lang.Object"))
                                    case "char" => (arg.removeQuotes(), Class.forName("java.lang.Char"))
                                    case "long" => (arg.toLong, Class.forName("java.lang.Long"))
                                    case "float" => (arg.toFloat, Class.forName("java.lang.Float"))
                                    case "double" => (arg.toDouble, Class.forName("java.lang.Double"))
                                    case "bool" | "boolean" => (if (arg.toLowerCase() == "true") true else false, Class.forName("java.lang.Boolean"))
                                    case _ => (arg.removeQuotes(), Class.forName(dataType))
                                }
                            }
                            else {
                                if (arg.quotesWith("\"")) {
                                    (arg.removeQuotes(), Class.forName("java.lang.String"))
                                }
                                else if (arg.quotesWith("'")) {
                                    (arg.removeQuotes(), Class.forName("java.lang.Char"))
                                }
                                else if (
                                    """^\d+$""".r.test(arg)) {
                                    (arg.toInt, Class.forName("java.lang.Integer"))
                                }
                                else if (
                                    """(?i)^\d+l$""".r.test(arg)) {
                                    (arg.toLong, Class.forName("java.lang.Long"))
                                }
                                else if (
                                    """(?i)^\d+f$""".r.test(arg)) {
                                    (arg.toFloat, Class.forName("java.lang.Float"))
                                }
                                else if (
                                    """(?i)^\d+d$""".r.test(arg)) {
                                    (arg.toDouble, Class.forName("java.lang.Double"))
                                }
                                else if (
                                    """(?i)true|false""".r.test(arg)) {
                                    (if (arg.toLowerCase() == "true") true else false, Class.forName("java.lang.Boolean"))
                                }
                                else {
                                    (arg, Class.forName("java.lang.Object"))
                                }
                            }
                        })

        java = java.$restore(PQL).takeBefore("(").trim()
        val method = java.takeAfterLast(".")
        val className = java.takeBeforeLast(".")

        Class.forName(className)
            .getDeclaredMethod(method, args.map(_._2): _*)
            .invoke(if (instance) {
                Class.forName(className).newInstance()
            }
            else {
                null
            }, args.map(_._1.asInstanceOf[Object]): _*)
    }
}
