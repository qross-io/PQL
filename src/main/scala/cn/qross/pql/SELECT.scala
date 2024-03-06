package cn.qross.pql

import cn.qross.core.{DataCell, DataTable, DataType}
import cn.qross.exception.SQLExecuteException
import cn.qross.ext.TypeExt._
import cn.qross.pql.Patterns.ARROW
import cn.qross.pql.Solver._

object SELECT {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("SELECT", sentence, new SELECT(sentence)))
    }
}

class SELECT(val sentence: String) {

    def evaluate(PQL: PQL, express: Int = Solver.FULL): DataCell = {
        sentence.$process(PQL, express, body => {
            val args = body.findArguments
            if (args.isEmpty) {
                val table = PQL.dh.executeDataTable(body)
                PQL.COUNT_OF_LAST_SELECT = table.size
                DataCell(table, DataType.TABLE)
            }
            else {
                throw new SQLExecuteException(s"Missed arguments: ${args.mkString(", ")}")
            }
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
        PQL.WORKING += evaluate(PQL).value
    }
}