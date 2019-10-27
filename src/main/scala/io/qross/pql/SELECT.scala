package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.ARROW
import io.qross.pql.Solver._

object SELECT {

    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("SELECT", sentence, new SELECT(sentence)))
    }
}

class SELECT(val sentence: String) {

    def select(PQL: PQL, express: Boolean = false): DataCell = {

        val (_select, links) =
            if (sentence.contains(ARROW)) {
                (sentence.takeBefore(ARROW), sentence.takeAfter(ARROW))
            }
            else {
                (sentence, "")
            }

        val data = PQL.dh.executeDataTable({
            if (express) {
                _select.$express(PQL).popStash(PQL)
            }
            else {
                _select.$restore(PQL)
            }
        }).toDataCell(DataType.TABLE)

        PQL.ROWS = data.dataType match {
            case DataType.TABLE => data.asTable.size
            case DataType.ARRAY | DataType.LIST => data.asList.size
            case _ => 1
        }

        if (links != "") {
            new Sharp(links, data).execute(PQL)
        }
        else {
            data
        }
    }

    def execute(PQL: PQL): Unit = {

        val result = this.select(PQL)

        PQL.RESULT += result.value

        if (PQL.dh.debugging) {
            Output.writeLine("                                                                        ")
            Output.writeLine(sentence.popStash(PQL))
            result.asTable.show()
        }
    }
}

/*
output match {
    case "TABLE" | "AUTO" => DataCell(PQL.dh.executeDataTable(query), DataType.TABLE)
    case "ROW" | "MAP" | "OBJECT" => DataCell(PQL.dh.executeDataTable(query).firstRow.getOrElse(DataRow()), DataType.ROW)
    case "LIST" | "ARRAY" => DataCell(PQL.dh.executeSingleList(query), DataType.ARRAY)
    case "VALUE" => PQL.dh.executeSingleValue(query)
}*/
