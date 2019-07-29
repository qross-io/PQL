package io.qross.sql

import io.qross.core.{DataRow, DataTable}
import io.qross.ext.TypeExt._

class FOR$EXPR(val variable: String, val caption: String, var expr: String) {

    private val fields = variable.split(",")
                                .map(v => {
                                    if (!v.trim.startsWith("$")) {
                                        throw new SQLParseException("In FOR loop variable must start with '$'.")
                                        ""
                                    }
                                    else {
                                        v.trim.takeAfter(0).$trim("(", ")")
                                    }
                                })

    if (caption == "SELECT") {
        expr = "SELECT " + expr
    }

    def computeMap(table: DataTable): ForVariables = {
        val loopVars = new ForVariables()

        if (fields.length <= table.columnCount) {
            table.map(row => {
                val newRow = DataRow()
                for (i <- fields.indices) {
                    newRow.set(fields(i).toUpperCase, row.get(i).orNull)
                }
                newRow
            }).foreach(loopVars.addRow)
        }
        else {
            throw new SQLParseException("In FOR loop, result columns must equal or more than variables amount.")
        }

        loopVars
    }
}
