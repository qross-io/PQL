package io.qross.pql

import io.qross.core.{DataCell, DataRow, DataTable, DataType}
import io.qross.exception.SQLParseException
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.pql.Patterns._
import io.qross.pql.Solver._

import scala.util.control.Breaks.{break, breakable}

object FOR {
    def parse(sentence: String, PQL: PQL): Unit = {
        $FOR.findFirstMatchIn(sentence) match {
            case Some(m) =>
                val $for: Statement = new Statement("FOR", m.group(0), new FOR(m.group(1).trim(), m.group(2).toUpperCase(), m.group(3).trim()))

                PQL.PARSING.head.addStatement($for)
                //只进栈
                PQL.PARSING.push($for)
                //待关闭的控制语句
                PQL.TO_BE_CLOSE.push($for)
                //继续解析子语句
                val first = sentence.takeAfter(m.group(0)).trim()
                if (first != "") {
                    PQL.parseStatement(first)
                }
            case None => throw new SQLParseException("Incorrect FOR sentence: " + sentence)
        }
    }
}

class FOR(val variable: String, val method: String, val collection: String) {

    val variables: Array[String] = variable.split(",").map(_.trim().toLowerCase())

    def execute(PQL: PQL, statement: Statement): Unit = {
        val vars: ForVariables = this.computeVariables(PQL)
        PQL.FOR$VARIABLES.push(vars)
        PQL.EXECUTING.push(statement)
        //根据loopMap遍历
        breakable {
            while (vars.hasNext) {
                if (!PQL.breakCurrentLoop) {
                    PQL.executeStatements(statement.statements)
                }
                else {
                    break
                }
            }
        }
    }

    private def computeVariables(PQL: PQL): ForVariables = {

        val forVars = new ForVariables()

        val data: DataCell = {
            if (collection.bracketsWith("(", ")")) {
                //集合查询语句
                collection.$trim("(", ")").trim().$compute(PQL)
            }
            else if ($VARIABLE.test(collection)) {
                //集合变量
                //@a, $b
                PQL.findVariable(collection)
            }
            else if (collection.bracketsWith("~json[", "]")) {
                val json = collection.$restore(PQL, "\"")
                if (json.bracketsWith("[", "]")) {
                    if (json.$trim("[", "]").trim().bracketsWith("{", "}")) {
                        Json(json).parseTable("/").toDataCell(DataType.TABLE)
                    }
                    else {
                        Json(json).parseJavaList("/").toDataCell(DataType.ARRAY)
                    }
                }
                else if (json.bracketsWith("{", "}")) {
                    Json(json).parseRow("/").toDataCell(DataType.ROW)
                }
                else {
                    DataCell(new DataTable(), DataType.TABLE)
                }
            }
            else {
                //SHARP表达式
                new Sharp(collection.$clean(PQL)).execute(PQL)
            }
        }


        data.dataType match {
            case DataType.TABLE =>
                data.asTable.foreach(row => {
                    val newRow = new DataRow()
                    if (method == "IN") {
                        for (i <- variables.indices) {
                            newRow.set(variables(i).drop(1).toUpperCase(), row.get(i).orNull)
                        }
                    }
                    else {
                        newRow.set(variables.head.drop(1).toUpperCase(), row, DataType.ROW)
                    }
                    forVars.addRow(newRow)
                })
            case DataType.ROW =>
                data.value.asInstanceOf[DataRow]
                    .turnToTable("key", "value")
                    .foreach(kv => {
                        val newRow = new DataRow()
                        if (method == "IN") {
                            for (i <- variables.indices) {
                                newRow.set(variables(i).drop(1).toUpperCase(), kv.get(i).orNull)
                            }
                        }
                        else {
                            newRow.set(variables.head.drop(1).toUpperCase(), kv, DataType.ROW)
                        }
                        forVars.addRow(newRow)
                    })
            case DataType.ARRAY | DataType.LIST =>
                data.asJavaList.forEach(item => {
                    val newRow = new DataRow()
                    for (i <- variables.indices) {
                        newRow.set(variables(i).drop(1).toUpperCase(), item)
                    }
                    forVars.addRow(newRow)
                })
            case DataType.HASHSET =>
                data.asHashSet.forEach(item => {
                    val newRow = new DataRow()
                    for (i <- variables.indices) {
                        newRow.set(variables(i).drop(1).toUpperCase(), item)
                    }
                    forVars.addRow(newRow)
                })
            case DataType.TEXT =>
                data.asText.split("").foreach(char => {
                    val newRow = new DataRow()
                    for (i <- variables.indices) {
                        newRow.set(variables(i).drop(1).toUpperCase(), char, DataType.TEXT)
                    }
                    forVars.addRow(newRow)
                })
            case value =>
                val newRow = new DataRow()
                for (i <- variables.indices) {
                    newRow.set(variables(i).drop(1).toUpperCase(), value)
                }
                forVars.addRow(newRow)
                //throw new SQLExecuteException("Unrecognized collection type: " + collection)
        }

        //如果变量的数量小于集合的列数，则变量赋值为null
        forVars
    }
}
