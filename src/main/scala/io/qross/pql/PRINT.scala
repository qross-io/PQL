package io.qross.pql

import java.util.regex.Matcher

import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.{$PRINT, $PRINT$SEAL, $BLANK}
import io.qross.pql.Solver._

object PRINT {
    val WARN: String = "WARN"
    val ERROR: String = "ERROR"
    val DEBUG: String = "DEBUG"
    val INFO: String = "INFO"
    val NONE: String = "NONE"

    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("PRINT", sentence, new PRINT(sentence.takeAfter($PRINT))))
    }
}

class PRINT(var message: String) {

    val messageType: String = {
        if ($PRINT$SEAL.test(message)) {
            val seal = message.takeBefore($BLANK).trim()
            message = message.takeAfter($PRINT$SEAL).trim()
            seal
        }
        else {
            "NONE"
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
            else {
                message.$eval(PQL).asText
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
