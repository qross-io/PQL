package io.qross.pql

import io.qross.core.{DataRow, DataTable}
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
import io.qross.net.Json._

class FOR(var variable: String, val collection: String) {

    val variables: List[String] = variable.split(",").map(_.trim).toList

    if (variable.contains(",")) {
        variable = variable.takeBefore(",").trim()
    }

    def computeVariables(PQL: PQL): ForVariables = {
        val forVars = new ForVariables()

        val table: DataTable =

                    if (collection.bracketsWith("(", ")")) {
                        //集合查询语句
                        //(SELECT...)
                        //(PARSE...)
                        val query = collection.$trim("(", ")").trim()
                        if ($SELECT.test(query)) {
                            new SELECT(query.$restore(PQL)).execute(PQL).asTable
                        }
                        else if ($PARSE.test(query)) {
                            PQL.dh.parseTable(query.takeAfter($PARSE).$eval(PQL).asText)
                        }
                        else {
                            throw new SQLExecuteException("Only supports SELECT or PARSE sentence in FOR loop query mode.")
                        }
                    }
                    else if (collection.bracketsWith("[", "]")) {
                        Json(collection.$restore(PQL, "\"")).parseTable("/")
                    }
                    else if (collection.bracketsWith("{", "}")) {
                        Json(collection.$restore(PQL, "\"")).parseRow("/").toTable()
                    }
                    else if ($VARIABLE.test(collection)) {
                        //集合变量
                        //@a, $b
                        PQL.findVariable(collection).asTable
                    }
                    else {
                        //SHARP表达式
                        new SHARP(collection.$clean(PQL)).execute(PQL).asTable
                    }

        if (variables.length <= table.columnCount) {
            table.map(row => {
                val newRow = new DataRow()
                for (i <- variables.indices) {
                    newRow.set(variables(i).substring(1).toUpperCase, row.get(i).orNull)
                }
                newRow
            }).foreach(forVars.addRow)
        }
        else {
            throw new SQLParseException("In FOR loop, result columns must equal or more than variables amount.")
        }

        forVars
    }
}
