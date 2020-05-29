package io.qross.pql

import io.qross.exception.{SQLExecuteException, SQLParseException}
import io.qross.ext.TypeExt._
import io.qross.fs.Excel._
import io.qross.fs.TextFile._
import io.qross.pql.Patterns.$OPEN
import io.qross.pql.Solver._

/*
OPEN "connectionName";
OPEN "connectionName" USE "databaseName";
OPEN CACHE;
OPEN TEMP;
OPEN DEFAULT USE "databaseName";
OPEN abbrName;
OPEN abbrName USE "databaseName";
*/

object OPEN {
    def parse(sentence: String, PQL: PQL): Unit = {
        $OPEN.findFirstIn(sentence) match {
            case Some(open) => PQL.PARSING.head.addStatement(new Statement("OPEN", sentence, new OPEN(sentence.takeAfter(open))))
            case None => throw new SQLParseException("Incorrect OPEN sentence: " + sentence)
        }
    }
}

class OPEN(val sentence: String) {

    def execute(PQL: PQL): Unit = {

        val plan = Syntax("OPEN").plan(sentence.$restore(PQL))

        plan.head match {
            case "CACHE" => PQL.dh.openCache()
            case "TEMP" => PQL.dh.openTemp()
            case "DEFAULT" =>
                PQL.dh.openDefault()
                if (plan.contains("AS")) {
                    PQL.dh.as(plan.oneArgs("AS"))
                }
                if (plan.contains("USE")) {
                    PQL.dh.use(plan.oneArgs("USE"))
                }
            case "QROSS" =>
                PQL.dh.openQross()
                if (plan.contains("USE")) {
                    PQL.dh.use(plan.oneArgs("USE"))
                }
            case "EXCEL" => PQL.dh.openExcel(plan.oneArgs("EXCEL"))
            case "JSON FILE" =>
                PQL.dh.openJsonFile(plan.headArgs)
                if (plan.contains("AS", "AS TABLE")) {
                    PQL.dh.asTable(plan.oneArgs("AS", "AS TABLE"))
                }
            case "TXT FILE" =>
                PQL.dh.openTextFile(plan.headArgs)
                if (plan.contains("WITH FIRST ROW HEADERS")) {
                    PQL.dh.withColumnsOfFirstRow()
                }
                else if (plan.contains("")) {
                    PQL.dh.withColumns(plan.multiArgs(""): _*)
                }
                else {
                    throw new SQLExecuteException("Must contains columns definition of table in OPEN TXT FILE: " + sentence)
                }
                if (plan.contains("BRACKETED BY")) {
                    val brackets = plan.limitArgs("BRACKETED BY")
                    PQL.dh.bracketedBy(brackets._1, brackets._2)
                }
                if (plan.contains("DELIMITED BY")) {
                    PQL.dh.delimitedBy(plan.oneArgs("DELIMITED BY"))
                }
                if (plan.contains("SKIP")) {
                    PQL.dh.skipLines(plan.oneArgs("SKIP").toInteger(0).toInt)
                }
                if (plan.contains("AS", "AS TABLE")) {
                    PQL.dh.asTable(plan.oneArgs("AS", "AS TABLE"))
                }
            case "CSV FILE" =>
                PQL.dh.openCsvFile(plan.headArgs)
                if (plan.contains("WITH FIRST ROW HEADERS")) {
                    PQL.dh.withColumnsOfFirstRow()
                }
                else if (plan.contains("")) {
                    PQL.dh.withColumns(plan.multiArgs(""): _*)
                }
                else {
                    throw new SQLExecuteException("Must contains columns definition of table in OPEN CSV FILE: " + sentence)
                }
                if (plan.contains("AS", "AS TABLE")) {
                    PQL.dh.asTable(plan.oneArgs("AS", "AS TABLE"))
                }
                if (plan.contains("BRACKETED BY")) {
                    val brackets = plan.limitArgs("BRACKETED BY")
                    PQL.dh.bracketedBy(brackets._1, brackets._2)
                }
                if (plan.contains("SKIP")) {
                    PQL.dh.skipLines(plan.oneArgs("SKIP").toInteger(0).toInt)
                }
            case "GZ FILE" =>
                if (plan.contains("WITH FIRST ROW HEADERS")) {
                    PQL.dh.withColumnsOfFirstRow()
                }
                else if (plan.contains("")) {
                    PQL.dh.withColumns(plan.multiArgs(""): _*)
                }
                else {
                    throw new SQLExecuteException("Must contains columns definition of table in OPEN GZ FILE: " + sentence)
                }
                if (plan.contains("AS", "AS TABLE")) {
                    PQL.dh.asTable(plan.oneArgs("AS", "AS TABLE"))
                }
                if (plan.contains("BRACKETED BY")) {
                    val brackets = plan.limitArgs("BRACKETED BY")
                    PQL.dh.bracketedBy(brackets._1, brackets._2)
                }
                if (plan.contains("DELIMITED BY")) {
                    PQL.dh.delimitedBy(plan.oneArgs("DELIMITED BY"))
                }
                if (plan.contains("SKIP")) {
                    PQL.dh.skipLines(plan.oneArgs("SKIP").toInteger(0).toInt)
                }
            case _ =>
                if (plan.contains("USE")) {
                    PQL.dh.open(plan.headArgs, plan.oneArgs("USE"))
                }
                else {
                    PQL.dh.open(plan.headArgs)
                }
                if (plan.contains("AS")) {
                    PQL.dh.as(plan.oneArgs("AS"))
                }
        }
    }
}
