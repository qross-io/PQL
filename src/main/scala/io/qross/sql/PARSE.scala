package io.qross.sql

import io.qross.core.{DataCell, DataType}
import io.qross.ext.TypeExt._
import io.qross.sql.Patterns.ARROW

class PARSE(val path: String) {

    def execute(PSQL: PSQL): DataCell = {

        val (select, links) =
            if (path.contains(ARROW)) {
                (path.takeBefore(ARROW), path.takeAfter(ARROW))
            }
            else {
                (path, "")
            }

        val data = DataCell(PSQL.dh.parseTable(path), DataType.TABLE)
        if (links != "") {
            new SHARP(links, data).execute(PSQL)
        }
        else {
            data
        }
    }

}

/*
query = query.takeAfter("""^PARSE\s""".r).trim.removeQuotes()
output match {
    case "TABLE" => DataCell(PSQL.dh.parseTable(query), DataType.TABLE)
    case "ROW" | "MAP" | "OBJECT" => DataCell(PSQL.dh.parseRow(query), DataType.ROW)
    case "LIST" | "ARRAY" => DataCell(PSQL.dh.parseList(query), DataType.ARRAY)
    case "VALUE" => PSQL.dh.parseValue(query)
    case "AUTO" => DataCell(PSQL.dh.parseNode(query), DataType.JSON)
}*/