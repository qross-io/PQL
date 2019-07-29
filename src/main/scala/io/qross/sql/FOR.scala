package io.qross.sql

import io.qross.core.{DataRow, DataTable}
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.sql.Patterns._
import io.qross.sql.Solver._

class FOR(var variable: String, val collection: String) {

    val variables: List[String] = variable.split(",").map(_.trim).toList

    if (variable.contains(",")) {
        variable = variable.takeBefore(",").trim()
    }

    def computeVariables(PSQL: PSQL): ForVariables = {
        val forVars = new ForVariables()

        val table: DataTable =

                    if (collection.bracketsWith("(", ")")) {
                        //集合查询语句
                        //(SELECT...)
                        //(PARSE...)
                        val query = collection.$trim("(", ")").trim()
                        if ($SELECT.test(query)) {
                            PSQL.dh.executeDataTable(query.$restore(PSQL))
                        }
                        else if ($PARSE.test(query)) {
                            PSQL.dh.parseTable(query.takeAfter($PARSE).$eval(PSQL).asText)
                        }
                        else {
                            throw new SQLExecuteException("Only supports SELECT or PARSE sentence in FOR loop query mode.")
                        }
                    }
                    else if (collection.bracketsWith("$", "}}")) {
//                        $list无意义
//                        $table无意义
//                        $value不能遍历
//                        $row有意义
                        var sentence = collection.$trim("$", "}}")
                        val resultType = sentence.takeBefore("{{").toUpperCase().trim()
                        sentence = sentence.takeAfter("{{")
                        val caption = sentence.takeBefore($BLANK).toUpperCase()

                        val table = if (caption == "SELECT") {
                                        PSQL.dh.executeDataTable(sentence.$restore(PSQL))
                                    }
                                    else if (caption == "PARSE") {
                                        PSQL.dh.parseTable(sentence.takeAfter($PARSE).$eval(PSQL).asText)
                                    }
                                    else {
                                        DataTable()
                                    }

                        if (Set("ROW", "OBJECT", "MAP").contains(resultType)) {
                            table.firstRow.getOrElse(DataRow()).toTable()
                        }
                        else {
                            table
                        }
                    }
                    else if (collection.bracketsWith("[", "]")) {
                        Json(collection.$place(PSQL)).parseTable("/")
                    }
                    else if (collection.bracketsWith("{", "}")) {
                        Json(collection.$place(PSQL)).parseRow("/").toTable()
                    }
                    else if ($VARIABLE.test(collection)) {
                        //集合变量
                        //@a, $b
                        PSQL.findVariable(collection).asTable
                    }
                    else {
                        //SHARP表达式
                        new SHARP(collection.$clean(PSQL)).execute(PSQL).asTable
                    }

        if (variables.length <= table.columnCount) {
            table.map(row => {
                val newRow = DataRow()
                for (i <- variables.indices) {
                    newRow.set(variables(i).toUpperCase, row.get(i).orNull)
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
