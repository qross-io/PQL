package cn.qross.pql

import java.util.regex.Matcher

import cn.qross.ext.Output
import cn.qross.ext.TypeExt._
import cn.qross.pql.Patterns.{$PRINT, $PRINT$SEAL, $BLANK}
import cn.qross.pql.Solver._

object PRINT {
    val WARN: String = "WARN"
    val ERROR: String = "ERROR"
    val DEBUG: String = "DEBUG"
    val INFO: String = "INFO"
    val NONE: String = "NONE"

    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("PRINT", sentence, new PRINT(sentence.takeAfterX($PRINT).trim())))
    }
}

class PRINT(var message: String) {

    val messageType: String = {
        $PRINT$SEAL.findFirstIn(message) match {
            case Some(seal) =>
                message = message.takeAfter(seal)
                seal.trim().toUpperCase
            case None => "NONE"
        }
    }

    def execute(PQL: PQL): Unit = {

        val info = {
            if (message.bracketsWith("(", ")")) {
                message
                        .$trim("(", ")")
                        .split(",")
                        .map(m => {
                            m.$eval(PQL).mkString("\"")
                        })
                        .mkString(", ")
                        .bracket("(", ")")
            }
            else if (message.bracketsWith("[", "]") || this.message.bracketsWith("{", "}")) {
                message.$restore(PQL, "\"")
            }
            else if (message != "") {
                message.$eval(PQL).asText
            }
            else {
                ""
            }
        }

        this.messageType match {
            case "WARN" => Output.writeWarning(info)
            case "ERROR" => Output.writeException(info)
            case "DEBUG" => Output.writeDebugging(info)
            case "INFO" => Output.writeMessage(info)
            case "NONE" => Output.writeLine(info)
            case seal: String => Output.writeLineWithSeal(seal, info)
            case _ =>
        }
    }
}
