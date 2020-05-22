package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.ext.TypeExt._
import io.qross.fs.Path._
import io.qross.fs.TextFile._
import io.qross.fs.Excel._
import io.qross.pql.Patterns._
import io.qross.pql.Solver._

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
                if (file.isFile) {
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
                    throw new SQLParseException("Incorrect csv file name or path at SAVE sentence: " + file)
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
                    throw new SQLParseException("Empty csv stream file name at SAVE sentence.")
                }
            case "TXT FILE" | "NEW TXT FILE" =>
                val file = plan.headArgs
                if (file.isFile) {
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
                    throw new SQLParseException("Incorrect text file name or path at SAVE sentence: " + file)
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
                if (file.isFile) {
                    if (plan.head.startsWith("NEW ") || action == "AS") {
                        PQL.dh.saveAsJsonFile(file)
                    }
                    else {
                        PQL.dh.saveToJsonFile(file)
                    }
                    PQL.dh.write()
                }
                else {
                    throw new SQLParseException("Incorrect json file name or path at SAVE sentence: " + file)
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
                if (file.isFile) {
                    if (plan.head.startsWith("NEW ") || action == "AS") {
                        PQL.dh.saveAsExcel(file)
                    }
                    else {
                        PQL.dh.saveToExcel(file)
                    }.useTemplate(plan.oneArgs("USE TEMPLATE", "TEMPLATE"))
                }
            case "EXCEL STREAM FILE" =>
                val fileName = plan.headArgs
                if (fileName != "") {
                    PQL.dh.saveAsStreamExcel(fileName).useTemplate(plan.oneArgs("USE TEMPLATE", "TEMPLATE"))
                }
            case "DEFAULT" =>
                PQL.dh.saveToDefault()
            case "QROSS" =>
                PQL.dh.saveToQross()
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