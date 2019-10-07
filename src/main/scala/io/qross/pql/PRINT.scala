package io.qross.pql

import java.util.regex.Matcher

import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.$PRINT
import io.qross.pql.Solver._

object PRINT {
    val WARN: String = "WARN"
    val ERROR: String = "ERROR"
    val DEBUG: String = "DEBUG"
    val INFO: String = "INFO"
    val NONE: String = "NONE"

    def parse(sentence: String, PQL: PQL): Unit = {
        val m: Matcher = $PRINT.matcher(sentence)
        if (m.find) {
            PQL.PARSING.head.addStatement(new Statement("PRINT", sentence, new PRINT(m.group(1), m.group(2).trim)))
        }
        else {
            throw new SQLParseException("Incorrect PRINT sentence: " + sentence)
        }
    }
}

class PRINT(var messageType: String, val message: String) {
    if (messageType == null) {
        messageType = "NONE"
    }
    else {
        messageType = messageType.trim.toUpperCase()
    }

    def execute(PQL: PQL): Unit = {

        val message = {
            if (this.message.bracketsWith("(", ")")) {
                this.message
                        .$trim("(", ")")
                        .split(",")
                        .map(m => {
                            m.$eval(PQL).mkString("\"")
                        })
                        .mkString(", ")
                        .bracket("(", ")")
            }
            else if (this.message.bracketsWith("[", "]") || this.message.bracketsWith("{", "}")) {
                this.message.$restore(PQL, "\"")
            }
            else {
                this.message.$eval(PQL).asText
            }
        }
        this.messageType match {
            case "WARN" => Output.writeWarning(message)
            case "ERROR" => Output.writeException(message)
            case "DEBUG" => Output.writeDebugging(message)
            case "INFO" => Output.writeMessage(message)
            case "NONE" => Output.writeLine(message)
            case seal: String => Output.writeLineWithSeal(seal, message)
            case _ =>
        }
    }
}
