package io.qross.sql

import io.qross.core.{DataCell, DataType}
import io.qross.ext.TypeExt._
import io.qross.sql.Patterns.ARROW

class SELECT(val sentence: String) {

    def execute(PSQL: PSQL): DataCell = {

        val (select, links) =
            if (sentence.contains(ARROW)) {
                (sentence.takeBefore(ARROW), sentence.takeAfter(ARROW))
            }
            else {
                (sentence, "")
            }

        val data = DataCell(PSQL.dh.executeDataTable(select), DataType.TABLE)
        if (links != "") {
            new SHARP(links, data).execute(PSQL)
        }
        else {
            data
        }
    }

}

/*
output match {
    case "TABLE" | "AUTO" => DataCell(PSQL.dh.executeDataTable(query), DataType.TABLE)
    case "ROW" | "MAP" | "OBJECT" => DataCell(PSQL.dh.executeDataTable(query).firstRow.getOrElse(DataRow()), DataType.ROW)
    case "LIST" | "ARRAY" => DataCell(PSQL.dh.executeSingleList(query), DataType.ARRAY)
    case "VALUE" => PSQL.dh.executeSingleValue(query)
}*/
