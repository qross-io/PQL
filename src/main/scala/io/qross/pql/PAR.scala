package io.qross.pql

import java.util.concurrent.ConcurrentLinkedDeque

import io.qross.core.DataRow
import io.qross.exception.SQLParseException
import io.qross.pql.Patterns.{$PAR, $RUN}
import io.qross.ext.TypeExt._
import io.qross.pql.Solver._
import io.qross.setting.Environment
import io.qross.thread.Parallel

object PAR {
    lazy val pipeline = new ConcurrentLinkedDeque[DataRow]()

    def parse(sentence: String, PQL: PQL): Unit = {
        $PAR.findFirstIn(sentence) match {
            case Some(par) => PQL.PARSING.head.addStatement(new Statement("PAR", sentence, new PAR(sentence.takeAfter(par).trim())))
            case None => throw new SQLParseException("Incorrect PAR sentence: " + sentence)
        }
    }
}

class PAR(val sentence: String) {
    def execute(PQL: PQL): Unit = {

        PQL.dh.foreach(row => {
            PAR.pipeline.add(row)
        }).clear()

        $RUN.findFirstMatchIn(sentence) match {
            case Some(m) =>
                val commandType = m.group(1).toUpperCase()
                var command = sentence.takeAfter(m.group(0)).trim().$restore(PQL)
                var args = ""
                commandType match {
                    case "PQL" =>
                        if (command.contains("=")) {
                            args = command.takeAfterLast(" ").trim().removeQuotes()
                            command = command.takeBeforeLast(" ").trim().removeQuotes()
                        }
                        else {
                            command = command.removeQuotes()
                        }
                    case "COMMAND" | "SHELL" =>
                        command = command.removeQuotes()
                }

                val parallel = new Parallel()
                for (i <- 0 until Environment.cpuThreads) {
                    parallel.add(new ParExecutor(commandType, command, args))
                }
                parallel.startAll()
                parallel.waitAll()

            case None => throw new SQLParseException("Incorrect RUN sentence: " + sentence)
        }
    }
}
