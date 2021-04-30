package io.qross.pql

import io.qross.core.DataCell
import io.qross.ext.ClassT
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
                    .split(",", -1)
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
                            dataType match {
                                case "String" => (arg.removeQuotes(), ClassT.$String)
                                case "int" => (arg.toInt, ClassT.$int)
                                case "Integer" => (arg.toInt, ClassT.$Integer)
                                case "Object" => (arg.removeQuotes(), ClassT.$Object)
                                case "char" => (arg.removeQuotes(), ClassT.$char)
                                case "Character" => (arg.removeQuotes(), ClassT.$Character)
                                case "long" => (arg.replaceAll("(?i)l$", "").toLong, ClassT.$long)
                                case "Long" => (arg.replaceAll("(?i)l$", "").toLong, ClassT.$Long)
                                case "float" => (arg.replaceAll("(?i)f$", "").toFloat, ClassT.$float)
                                case "Float" => (arg.replaceAll("(?i)f$", "").toFloat, ClassT.$Float)
                                case "double" => (arg.replaceAll("(?i)d$", "").toDouble, ClassT.$double)
                                case "Double" => (arg.replaceAll("(?i)d$", "").toDouble, ClassT.$Double)
                                case "boolean" => (if (arg.toLowerCase() == "true") true else false, ClassT.$boolean)
                                case "Boolean" => (if (arg.toLowerCase() == "true") true else false, ClassT.$Boolean)
                                case _ => (arg.removeQuotes(), Class.forName(dataType))
                            }
                        }
                        else {
                            if (arg.quotesWith("\"")) {
                                (arg.removeQuotes(), ClassT.$String)
                            }
                            else if (arg.quotesWith("'")) {
                                (arg.removeQuotes(), ClassT.$char)
                            }
                            else if ("""^\d+$""".r.test(arg)) {
                                (arg.toInt, ClassT.$int)
                            }
                            else if ("""(?i)^\d+l$""".r.test(arg)) {
                                (arg.dropRight(1).toLong, ClassT.$long)
                            }
                            else if ("""(?i)\d+f$""".r.test(arg)) {
                                (arg.dropRight(1).toFloat, ClassT.$float)
                            }
                            else if ("""^\d+\.\d+$""".r.test(arg)) {
                                (arg.toFloat, ClassT.$float)
                            }
                            else if ("""(?i)\d+d$""".r.test(arg)) {
                                (arg.dropRight(1).toDouble, ClassT.$double)
                            }
                            else if ("""(?i)^(true|false)$""".r.test(arg)) {
                                (if (arg.toLowerCase() == "true") true else false, ClassT.$boolean)
                            }
                            else {
                                (arg, ClassT.$Object)
                            }
                        }
                    })

                java = java.takeBefore("(")
                val method = java.takeAfterLast(".")
                val className = java.takeBeforeLast(".")

                DataCell(Class.forName(className)
                    .getDeclaredMethod(method, args.map(_._2): _*)
                    .invoke(null, args.map(_._1.asInstanceOf[Object]): _*))
            }
            else {
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
