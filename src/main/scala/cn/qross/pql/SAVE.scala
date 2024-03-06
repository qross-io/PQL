package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.ext.TypeExt._
import cn.qross.fs.Excel._
import cn.qross.fs.TextFile._
import cn.qross.net.Redis._
import cn.qross.pql.Patterns._
import cn.qross.pql.Solver._

object SAVE {
    def parse(sentence: String, PQL: PQL): Unit = {
        $SAVE.findFirstMatchIn(sentence) match {
            case Some(m) => PQL.PARSING.head.addStatement(new Statement("SAVE", sentence, new SAVE(m.group(1).toUpperCase(), sentence.takeAfter(m.group(0)).trim())))
            case None => throw new SQLParseException("Incorrect SAVE sentence: " + sentence)
        }
    }
}

class SAVE(val action: String, val sentence: String) {

    def execute(PQL: PQL): Unit = {

        val plan = Syntax("SAVE " + action).plan(sentence.$restore(PQL))

        plan.head match {
            case "CACHE" => PQL.dh.saveToCache()
            case "CACHE TABLE" =>
                if (plan.size == 1) {
                    PQL.dh.cache(plan.headArgs)
                }
                else {
                    PQL.dh.cache(plan.headArgs, plan.lastArgs)
                }
            case "TEMP" => PQL.dh.saveToTemp()
            case "TEMP TABLE" =>
                if (plan.size == 1) {
                    PQL.dh.temp(plan.headArgs)
                }
                else {
                    PQL.dh.temp(plan.headArgs, plan.options.map(phrase => (phrase, plan.multiArgs(phrase).mkString(","))).toSeq: _*)
                }
            case "CSV FILE" | "NEW CSV FILE" =>
                val file = plan.headArgs
                if (file != "") {
                    if (plan.head.startsWith("NEW ") || action == "AS") {
                        PQL.dh.saveAsCsvFile(file)
                    }
                    else {
                        PQL.dh.saveToCsvFile(file)
                    }

                    if (plan.contains("WITHOUT HEADERS")) {
                        PQL.dh.withoutHeaders()
                    }
                    else if (plan.contains("WITH HEADERS")) {
                        PQL.dh.withHeaders(plan.selectArgs("WITH HEADERS"): _*)
                    }
                    else {
                        PQL.dh.withHeaders()
                    }
                    PQL.dh.write()
                }
                else {
                    throw new SQLParseException("Empty CSV file name at SAVE sentence.")
                }
            case "CSV STREAM FILE" =>
                val file = plan.headArgs
                if (file != "") {
                    PQL.dh.saveAsStreamCsvFile(file)
                    if (plan.contains("WITHOUT HEADERS")) {
                        PQL.dh.withoutHeaders()
                    }
                    else if (plan.contains("WITH HEADERS")) {
                        PQL.dh.withHeaders(plan.selectArgs("WITH HEADERS"): _*)
                    }
                    else {
                        PQL.dh.withHeaders()
                    }
                    PQL.dh.write()
                }
                else {
                    throw new SQLParseException("Empty CSV stream file name at SAVE sentence.")
                }
            case "TXT FILE" | "NEW TXT FILE" =>
                val file = plan.headArgs
                if (file != "") {
                    if (plan.head.startsWith("NEW ") || action == "AS") {
                        PQL.dh.saveAsTextFile(file)
                    }
                    else {
                        PQL.dh.saveToTextFile(file)
                    }

                    if (plan.contains("DELIMITED BY")) {
                        PQL.dh.delimit(plan.get("DELIMITED BY").getOrElse(",").removeQuotes())
                    }

                    if (plan.contains("WITHOUT HEADERS")) {
                        PQL.dh.withoutHeaders()
                    }
                    else if (plan.contains("WITH HEADERS")) {
                        PQL.dh.withHeaders(plan.selectArgs("WITH HEADERS"): _*)
                    }
                    else {
                        PQL.dh.withHeaders()
                    }

                    PQL.dh.write()
                }
                else {
                    throw new SQLParseException("Empty TXT file name or path at SAVE sentence: " + file)
                }
            case "STREAM FILE" | "TXT STREAM FILE" =>
                val file = plan.headArgs
                if (file != "") {
                    PQL.dh.saveAsStreamFile(file)

                    if (plan.contains("DELIMITED BY")) {
                        PQL.dh.delimit(plan.get("DELIMITED BY").getOrElse(",").removeQuotes())
                    }

                    if (plan.contains("WITHOUT HEADERS")) {
                        PQL.dh.withoutHeaders()
                    }
                    else if (plan.contains("WITH HEADERS")) {
                        PQL.dh.withHeaders(plan.selectArgs("WITH HEADERS"): _*)
                    }
                    else {
                        PQL.dh.withHeaders()
                    }
                    PQL.dh.write()
                }
                else {
                    throw new SQLParseException("Empty stream file name at SAVE sentence.")
                }
            case "JSON FILE" | "NEW JSON FILE" =>
                val file = plan.headArgs
                if (file != "") {
                    if (plan.head.startsWith("NEW ") || action == "AS") {
                        PQL.dh.saveAsJsonFile(file)
                    }
                    else {
                        PQL.dh.saveToJsonFile(file)
                    }
                    PQL.dh.write()
                }
                else {
                    throw new SQLParseException("Empty JSON file name or path at SAVE sentence: " + file)
                }
            case "JSON STREAM FILE" =>
                val file = plan.headArgs
                if (file != "") {
                    PQL.dh.saveAsStreamJsonFile(file).write()
                }
                else {
                    throw new SQLParseException("Empty json stream file name at SAVE sentence.")
                }
            case "EXCEL" | "NEW EXCEL" =>
                val file = plan.headArgs
                if (file != "") {
                    if (plan.head.startsWith("NEW ") || action == "AS") {
                        PQL.dh.saveAsExcel(file)
                    }
                    else {
                        PQL.dh.saveToExcel(file)
                    }

                    if (plan.contains("USE DEFAULT TEMPLATE", "DEFAULT TEMPLATE")) {
                        PQL.dh.useDefaultExcelTemplate()
                    }
                    else if (plan.contains("USE TEMPLATE", "TEMPLATE")) {
                        PQL.dh.useExcelTemplate(plan.oneArgs("USE TEMPLATE", "TEMPLATE"))
                    }
                }
                else {
                    throw new SQLParseException("Empty Excel file name at SAVE sentence.")
                }
            case "EXCEL STREAM FILE" =>
                val file = plan.headArgs
                if (file != "") {
                    PQL.dh.saveAsStreamExcel(file)

                    if (plan.contains("USE DEFAULT TEMPLATE", "DEFAULT TEMPLATE")) {
                        PQL.dh.useDefaultExcelTemplate()
                    }
                    else if (plan.contains("USE TEMPLATE", "TEMPLATE")) {
                        PQL.dh.useExcelTemplate(plan.oneArgs("USE TEMPLATE", "TEMPLATE"))
                    }
                }
                else {
                    throw new SQLParseException("Empty Excel stream file name at SAVE sentence.")
                }
            case "DEFAULT" =>
                PQL.dh.saveToDefault()
            case "QROSS" =>
                PQL.dh.saveToQross()
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
                PQL.dh.saveTo(connectionName, driver, plan.headArgs, username, password, use)
            case "REDIS" =>
                if (plan.contains("SELECT")) {
                    val db = plan.oneArgs("SELECT")
                    if ("""^\d+$""".r.test(db)) {
                        PQL.dh.saveToRedis(plan.headArgs, db.toInt)
                    }
                    else {
                        throw new SQLParseException("Incorrect database index: " + db + ". + It's must be a integer.")
                    }
                }
                else {
                    PQL.dh.saveToRedis(plan.headArgs)
                }
            case _ =>
                val connectionName = plan.headArgs
                if (plan.size == 1) {
                    PQL.dh.saveTo(connectionName)
                }
                else if (plan.last == "USE") {
                    PQL.dh.saveTo(connectionName, plan.lastArgs)
                }
        }
    }
}