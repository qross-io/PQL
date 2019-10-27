package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.ext.Output
import io.qross.pql.Patterns.{$DEBUG, $DELETE, $DELETE$FILE, ARROW}
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._
import io.qross.fs.FilePath._

object DELETE {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("DELETE", sentence, new DELETE(sentence)))
    }
}

class DELETE(var sentence: String) {

    val deleteType: String = {
        if ($DELETE$FILE.test(sentence)) {
            sentence = sentence.takeAfter($DELETE$FILE)
            "FILE"
        }
        else {
            "JDBC"
        }
    }

    def delete(PQL: PQL, express: Boolean = false): DataCell = {
        deleteType match {
            case "JDBC" =>
                val (_delete, links) =
                    if (sentence.contains(ARROW)) {
                        (sentence.takeBefore(ARROW), sentence.takeAfter(ARROW))
                    }
                    else {
                        (sentence, "")
                    }

                val data = PQL.dh.executeNonQuery({
                    if (express) {
                        _delete.$express(PQL).popStash(PQL)
                    }
                    else {
                        _delete.$restore(PQL)
                    }

                }).toDataCell(DataType.INTEGER)

                PQL.AFFECTED = data.value.asInstanceOf[Int]

                if (links != "") {
                    new Sharp(links, data).execute(PQL)
                }
                else {
                    data
                }
            case "FILE" =>
                val path = sentence.$eval(PQL)
                PQL.BOOL = path.asText.delete()

                if (PQL.dh.debugging) {
                    if (PQL.BOOL) {
                        Output.writeDebugging(s"File $path has been removed.")
                    }
                    else {
                        Output.writeDebugging(s"File $path can be deleted. Maybe it does not exist or in use.")
                    }
                }
                DataCell(PQL.BOOL, DataType.BOOLEAN)
            case _ => DataCell.NULL
        }
    }

    def execute(PQL: PQL): Unit = {
        this.delete(PQL)
        deleteType match {
            case "JDBC" =>
                if (PQL.dh.debugging) {
                    Output.writeLine("                                                                        ")
                    Output.writeLine(sentence)
                    Output.writeLine("------------------------------------------------------------------------")
                    Output.writeLine(s"${PQL.AFFECTED} row(s) affected. ")
                }
            case _ =>
        }
    }
}
