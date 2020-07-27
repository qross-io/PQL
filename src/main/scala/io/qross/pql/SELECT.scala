package io.qross.pql

import io.qross.core.{DataCell, DataTable, DataType}
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.ARROW
import io.qross.pql.Solver._

object SELECT {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("SELECT", sentence, new SELECT(sentence)))
    }
}

class SELECT(val sentence: String) {

    def select(PQL: PQL, express: Int = Solver.FULL): DataCell = {
        sentence.$process(PQL, express, body => {
            val table = PQL.dh.executeDataTable(body)
            PQL.COUNT_OF_LAST_SELECT = table.size
            DataCell(table, DataType.TABLE)
        })
    }

    //用于pass语句中
    def select(PQL: PQL, table: DataTable): DataCell = {

        var body = sentence.$clean(PQL)
        val links = {
            if (body.contains(ARROW)) {
                body.takeAfter(ARROW)
            }
            else {
                ""
            }
        }

        if (links != "") {
            body = body.takeBefore(ARROW).trim()
        }

        val data = PQL.dh.tableSelect(body.popStash(PQL), table)
        PQL.COUNT_OF_LAST_SELECT = data.size

        if (links != "") {
            new Sharp(links, DataCell(data, DataType.TABLE)).execute(PQL)
        }
        else {
            DataCell(data, DataType.TABLE)
        }
    }

    def execute(PQL: PQL): Unit = {
        PQL.WORKING += this.select(PQL).value
    }
}