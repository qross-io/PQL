package io.qross.pql

import io.qross.core.{DataCell, DataRow, DataType}
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.$RUN
import io.qross.pql.Solver._
import io.qross.script.Shell._

object RUN {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("RUN", sentence, new RUN(sentence)))
    }
}

class RUN(val sentence: String) {

    def evaluate(PQL: PQL, express: Int = Solver.FULL): DataCell = {
        $RUN.findFirstMatchIn(sentence) match {
            case Some(m) =>
                m.group(1).toUpperCase() match {
                    case "PQL" =>
                        sentence.$process(PQL, express, body => {
                            val command = body.takeAfter(m.group(0)).trim().removeQuotes()
                            val (file, args) = {
                                if (command.contains("?")) {
                                    (command.takeBefore("?"), command.takeAfter("?"))
                                }
                                else {
                                    (command, "")
                                }
                            }
                            io.qross.pql.PQL.openFile(file).place(args).run().toDataCell
                        })
                    case "COMMAND" | "SHELL" =>
                        sentence.$process(PQL, express, body => {
                            DataCell(body.takeAfter(m.group(0)).trim().removeQuotes().run(), DataType.ROW)
                        }, "\"")
                    case _ => DataCell.NULL
                }
            case None =>DataCell.NULL
        }
    }

    def execute(PQL: PQL): Unit = {
        PQL.WORKING += evaluate(PQL).value.asInstanceOf[DataRow]
    }
}
