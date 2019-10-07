package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.ARROW
import io.qross.pql.Solver._

object SELECT {

    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("SELECT", sentence))
    }
}

class SELECT(val sentence: String) {

    val (select, links) =
        if (sentence.contains(ARROW)) {
            (sentence.takeBefore(ARROW), sentence.takeAfter(ARROW))
        }
        else {
            (sentence, "")
        }

    def query(PQL: PQL): DataCell = {
       val data = PQL.dh.executeDataTable(select.$restore(PQL)).toDataCell(DataType.TABLE)
        if (links != "") {
            new SHARP(links, data).execute(PQL)
        }
        else {
            data
        }
    }

    def execute(PQL: PQL): Unit = {
        val result = this.query(PQL)
        PQL.RESULT += result.value
        PQL.ROWS = result.dataType match {
            case DataType.TABLE => result.asTable.size
            case DataType.ARRAY | DataType.LIST => result.asList.size
            case _ => 1
        }

        if (PQL.dh.debugging) {
            Output.writeLine("                                                                        ")
            Output.writeLine(sentence.popStash(PQL).take(100))
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
