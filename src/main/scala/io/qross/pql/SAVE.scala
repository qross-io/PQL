package io.qross.pql

import io.qross.ext.TypeExt._
import io.qross.fs.Path._
import io.qross.fs.TextFile._
import io.qross.fs.Excel._
import io.qross.pql.Patterns._
import io.qross.pql.Solver._

object SAVE {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($SAVE$AS.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("SAVE", sentence, new SAVE(sentence.takeAfter($SAVE$AS))))
        }
        else {
            throw new SQLParseException("Incorrect SAVE sentence: " + sentence)
        }
    }
}

class SAVE(var sentence: String) {

    def execute(PQL: PQL): Unit = {

        sentence = sentence.$restore(PQL)

        val plan = Syntax("SAVE AS").plan(sentence)

        plan.head.words match {
            case "CACHE" => PQL.dh.saveAsCache()
            case "CACHE TABLE" =>
                if (plan.size == 1) {
                    PQL.dh.cache(plan.head.oneArgs)
                }
                else {
                    PQL.dh.cache(plan.head.oneArgs, plan.last.oneArgs)
                }
            case "TEMP" => PQL.dh.saveAsTemp()
            case "TEMP TABLE" =>
                if (plan.size == 1) {
                    PQL.dh.temp(plan.head.oneArgs)
                }
                else {
                    PQL.dh.temp(plan.head.oneArgs, plan.options.map(p => (p.words, p.multiArgs.mkString(","))): _*)
                }
            case "CSV FILE" | "NEW CSV FILE" =>
                val file = plan.head.oneArgs
                if (file.isFile) {
                    if (plan.head.words.startsWith("NEW ")) {
                        PQL.dh.saveAsNewCsvFile(file)
                    }
                    else {
                        PQL.dh.saveAsCsvFile(file)
                    }

                    PQL.dh.withHeaders()
                    plan.iterate(phrase => {
                        phrase.words match {
                            case "WITHOUT HEADERS" => PQL.dh.withoutHeaders()
                            case "WITH HEADERS" => PQL.dh.withHeaders(phrase.multiArgs)
                        }
                    })
                    PQL.dh.write()
                }
                else {
                    throw new SQLParseException("Incorrect csv file name or path at SAVE AS: " + file)
                }
            case "TXT FILE" | "NEW TXT FILE" =>
                val file = plan.head.oneArgs
                if (file.isFile) {
                    if (plan.head.words.startsWith("NEW ")) {
                        PQL.dh.saveAsNewTextFile(file)
                    }
                    else {
                        PQL.dh.saveAsTextFile(file)
                    }

                    PQL.dh.withHeaders()
                    plan.iterate(phrase => {
                        phrase.words match {
                            case "WITHOUT HEADERS" => PQL.dh.withoutHeaders()
                            case "WITH HEADERS" => PQL.dh.withHeaders(phrase.multiArgs)
                            case "DELIMITED BY" => PQL.dh.delimit(phrase.oneArgs)
                        }
                    })
                    PQL.dh.write()
                }
                else {
                    throw new SQLParseException("Incorrect text file name or path at SAVE AS: " + file)
                }
            case "JSON FILE" | "NEW JSON FILE" =>
                val file = plan.head.oneArgs
                if (file.isFile) {
                    if (plan.head.words.startsWith("NEW ")) {
                        PQL.dh.saveAsNewJsonFile(file)
                    }
                    else {
                        PQL.dh.saveAsJsonFile(file)
                    }
                    PQL.dh.write()
                }
                else {
                    throw new SQLParseException("Incorrect json file name or path at SAVE AS: " + file)
                }
            case "EXCEL" | "NEW EXCEL" =>
                val file = plan.head.oneArgs
                if (file.isFile) {
                    if (plan.head.words.startsWith("NEW ")) {
                        PQL.dh.saveAsNewExcel(file)
                    }
                    else {
                        PQL.dh.saveAsExcel(file)
                    }

                    plan.iterate(phrase => {
                        phrase.words match {
                            case "USE DEFAULT TEMPLATE" => PQL.dh.useDefaultTemplate()
                            case "USE TEMPLATE" => PQL.dh.useTemplate(phrase.oneArgs)
                        }
                    })
                }
            case "DEFAULT" =>
                PQL.dh.saveAsDefault()
            case "QROSS" =>
                PQL.dh.saveAsQross()
            case _ =>
                val connectionName = plan.head.oneArgs
                if (plan.size == 1) {
                    PQL.dh.saveAs(connectionName)
                }
                else if ( plan.last.words == "USE") {
                    PQL.dh.saveAs(connectionName, plan.last.oneArgs)
                }
        }
    }
}