package cn.qross.pql

import cn.qross.exception.{SQLExecuteException, SQLParseException}
import cn.qross.ext.TypeExt._
import cn.qross.fs.Excel._
import cn.qross.fs.TextFile._
import cn.qross.pql.Patterns.$OPEN
import cn.qross.pql.Solver._
import cn.qross.net.Redis._

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
        PQL.PARSING.head.addStatement(new Statement("OPEN", sentence, new OPEN(sentence.takeAfterX($OPEN))))
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
            case "DATABASE" =>
                val connectionName = {
                    if (plan.contains("AS")) {
                        plan.oneArgs("AS")
                    }
                    else {
                        "$TEMP$CONNECTION"
                    }
                }
                val username = {
                    if (plan.contains("USERNAME")) {
                        plan.oneArgs("USERNAME")
                    }
                    else {
                        ""
                    }
                }
                val password = {
                    if (plan.contains("PASSWORD")) {
                        plan.oneArgs("PASSWORD")
                    }
                    else {
                        ""
                    }
                }
                val driver = {
                    if (plan.contains("DRIVER")) {
                        plan.oneArgs("DRIVER")
                    }
                    else {
                        ""
                    }
                }
                val use = {
                    if (plan.contains("USE")) {
                        plan.oneArgs("USE")
                    }
                    else {
                        ""
                    }
                }
                PQL.dh.open(connectionName, driver, plan.headArgs, username, password, use)
            case "REDIS" =>
                if (plan.contains("SELECT")) {
                    val db = plan.oneArgs("SELECT")
                    if ("""^\d+$""".r.test(db)) {
                        PQL.dh.openRedis(plan.headArgs.takeAfterIfContains("redis."), db.toInt)
                    }
                    else {
                        throw new SQLParseException("Incorrect database index: " + db + ". + It's must be a integer.")
                    }
                }
                else {
                    PQL.dh.openRedis(plan.headArgs)
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
