package io.qross.pql

import io.qross.exception.{SQLExecuteException, SQLParseException}
import io.qross.time.Timer
import io.qross.time.TimeSpan._
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns._
import io.qross.pql.Solver._

object SLEEP {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("SLEEP", sentence, new SLEEP(sentence.takeAfterX($SLEEP).trim())))
    }
}

class SLEEP(timeSpan: String) {

    def execute(PQL: PQL): Unit = {
        if ("""(?i)TO\s+NEXT\s+SECOND""".r.test(timeSpan)) {
            Timer.sleepToNextSecond()
        }
        else if ("""(?i)TO\s+NEXT\s+MINUTE""".r.test(timeSpan)) {
            Timer.sleepToNextMinute()
        }
        else if ($BLANK.test(timeSpan)) {
            val m = timeSpan.takeBeforeX($BLANK).$eval(PQL).asDecimal
            timeSpan.takeAfterLastX($BLANK).trim().toUpperCase match {
                case "MILLIS" | "MILLI" => Timer.sleep(m millis)
                case "SECONDS" | "SECOND" => Timer.sleep(m seconds)
                case "MINUTES" | "MINUTE" => Timer.sleep(m minutes)
                case "HOURS" | "DAYS" | "HOUR" | "DAY" => throw new SQLExecuteException("The unit of time is too large. Please use MILLIS, SECONDS or MINUTES")
                case unit => throw new SQLExecuteException("The unit of time is wrong:" + unit)
            }
        }
        else {
            val millis = timeSpan.takeBeforeX($BLANK).$eval(PQL).asInteger
            Timer.sleep(millis)
        }
    }
}