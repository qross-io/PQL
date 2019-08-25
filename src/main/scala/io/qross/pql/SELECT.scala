package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.ARROW

class SELECT(val sentence: String) {

    def execute(PQL: PQL): DataCell = {

        val (select, links) =
            if (sentence.contains(ARROW)) {
                (sentence.takeBefore(ARROW), sentence.takeAfter(ARROW))
            }
            else {
                (sentence, "")
            }

        val data = DataCell(PQL.dh.executeDataTable(select), DataType.TABLE)
        if (links != "") {
            new SHARP(links, data).execute(PQL)
        }
        else {
            data
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