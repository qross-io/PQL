package io.qross.pql

import io.qross.core.DataCell
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.$BLANK
import io.qross.pql.Solver._

import scala.collection.mutable

object INVOKE {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("INVOKE", sentence, new INVOKE(sentence)))
    }
}

class INVOKE(sentence: String) {

    def evaluate(PQL: PQL, express: Int = Solver.FULL): DataCell = {
        sentence.$process(PQL, express, body => {
            //body 已经是恢复完成的语句
            val chars = new mutable.ListBuffer[String]
            var java = body.drop(6).trim().pickChars(chars)

            if (java.contains("(")) {
                val args = java.takeAfter("(")
                    .takeBeforeLast(")")
                    .split(",")
                    .map(_.trim)
                    .map(item => {
                        val (arg, dataType) = {
                            $BLANK.findFirstIn(item) match {
                                case Some(blank) =>
                                    (item.takeAfter(blank).trim().restoreChars(chars), item.takeBefore(blank).trim())
                                case None => (item.restoreChars(chars), "")
                            }
                        }

                        if (dataType != "") {
                            dataType.toLowerCase() match {
                                case "string" => (arg.removeQuotes(), Class.forName("java.lang.String"))
                                case "int" | "integer" => (arg.toInt, Class.forName("java.lang.Integer"))
                                case "object" => (arg.removeQuotes(), Class.forName("java.lang.Object"))
                                case "char" | "character" => (arg.removeQuotes(), Class.forName("java.lang.Character"))
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
                                (arg.removeQuotes(), Class.forName("java.lang.Character"))
                            }
                            else if ("""^\d+$""".r.test(arg)) {
                                (arg.toInt, Class.forName("java.lang.Integer"))
                            }
                            else if ("""(?i)^\d+l$""".r.test(arg)) {
                                (arg.toLong, Class.forName("java.lang.Long"))
                            }
                            else if ("""(?i)^\d+f$""".r.test(arg)) {
                                (arg.toFloat, Class.forName("java.lang.Float"))
                            }
                            else if ("""(?i)^\d+d$""".r.test(arg)) {
                                (arg.toDouble, Class.forName("java.lang.Double"))
                            }
                            else if ("""(?i)^(true|false)$""".r.test(arg)) {
                                (if (arg.toLowerCase() == "true") true else false, Class.forName("java.lang.Boolean"))
                            }
                            else {
                                (arg, Class.forName("java.lang.Object"))
                            }
                        }
                    }).filter(_._1 != "")

                java = java.takeBefore("(")
                val method = java.takeAfterLast(".")
                val className = java.takeBeforeLast(".")

                DataCell(Class.forName(className)
                    .getDeclaredMethod(method, args.map(_._2): _*)
                    .invoke(null, args.map(_._1.asInstanceOf[Object]): _*))
            }
            else {
                val java = body.drop(6).trim()
                val field = java.takeAfterLast(".")
                val className = java.takeBeforeLast(".")

                DataCell(Class.forName(className)
                    .getDeclaredField(field)
                    .get(null))
            }
        }, "\"")
    }

    def execute(PQL: PQL): Unit = {
        PQL.WORKING += evaluate(PQL).value
    }
}
