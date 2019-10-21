package io.qross.pql

import io.qross.core.{DataRow, DataTable}
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
import io.qross.net.Json._

import scala.util.control.Breaks.{break, breakable}

object FOR {
    def parse(sentence: String, PQL: PQL): Unit = {
        val m = $FOR.matcher(sentence)
        if (m.find) {
            val $for: Statement = new Statement("FOR", m.group(0), new FOR(m.group(1).trim(), m.group(2).trim()))

            PQL.PARSING.head.addStatement($for)
            //只进栈
            PQL.PARSING.push($for)
            //待关闭的控制语句
            PQL.TO_BE_CLOSE.push($for)
            //继续解析子语句
            PQL.parseStatement(sentence.takeAfter(m.group(0)).trim())
        }
        else {
            throw new SQLParseException("Incorrect FOR sentence: " + sentence)
        }
    }
}

class FOR(var variable: String, val collection: String) {

    def execute(PQL: PQL, statement: Statement): Unit = {
        val vars: ForVariables = this.computeVariables(PQL)
        PQL.FOR$VARIABLES.push(vars)
        PQL.EXECUTING.push(statement)
        //根据loopMap遍历/
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


    val variables: List[String] = variable.split(",").map(_.trim).toList

    if (variable.contains(",")) {
        variable = variable.takeBefore(",").trim()
    }

    private def computeVariables(PQL: PQL): ForVariables = {
        val forVars = new ForVariables()

        val table: DataTable =
                    if (collection.bracketsWith("(", ")")) {
                        //集合查询语句
                        //(SELECT...)
                        //(PARSE...)
                        val query = collection.$trim("(", ")").trim()
                        if ($SELECT.test(query)) {
                            new SELECT(query).query(PQL).asTable
                        }
                        else if ($PARSE.test(query)) {
                            new PARSE(query).parse(PQL).asTable
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
                        new Sharp(collection.$clean(PQL)).execute(PQL).asTable
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
