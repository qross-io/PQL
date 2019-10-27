package io.qross.pql

import io.qross.setting.Properties
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
import io.qross.fs.FilePath._
import io.qross.fs.TextFile._

import scala.util.matching.Regex

/*
SAVE AS connectionName;
SAVE AS connectionName USE databaseName;
SAVE AS DEFAULT;
SAVE AS QROSS;

SAVE AS CACHE;
SAVE AS CACHE TABLE tableName;
SAVE AS CACHE TABLE tableName
    PRIMARY KEY id;

SAVE AS TEMP;
SAVE AS TEMP TABLE tableName;
SAVE AS TEMP TABLE tableName
    PRIMARY KEY id
    UNIQUE KEY (a, b, c)
    KEY (c,d);

SAVE AS (NEW) CSV FILE "file.csv"
    WITHOUT HEADER
    WITH HEADER { "name": "label" };
SAVE AS (NEW) JSON FILE "file.json";
SAVE AS TXT FILE "file.log"
    WITH HEADER
    WITH HEADER { "name": "label" }
    DELIMITED BY ",";

SAVE AS NEW EXCEL "abc.xlxs";


*/

object SAVE {
//    val DEFAULT: Regex = """(?i)^DEFAULT$""".r
//    val CACHE: Regex = """(?i)^CACHE$""".r
//    val CACHE$TABLE: Regex = """(?i)^CACHE\s+TABLE\s$""".r
//    val TEMP: Regex = """(?i)^TEMP$""".r
//    val TEMP$TABLE: Regex = """(?i)^TEMP\s+TABLE\s$""".r
    val NEW: Regex = """(?i)^NEW\s+""".r
    val CSV$FILE: Regex = """(?i)^CSV\s+FILE\s""".r
    val JSON$FILE: Regex = """(?i)^JSON\s+FILE\s""".r
    val TXT$FILE: Regex = """(?i)^TE?XT\s+FILE\s""".r
    val WITH$HEADER: Regex = """(?i)\sWITH\s+HEADER\s""".r
    val WITH$HEADER$: Regex = """(?i)\sWITH\s+HEADER$""".r
    val DELIMITED$BY: Regex = """(?i)\sDELIMITED\s+BY\s""".r
    val EXCEL: Regex = """(?i)EXCEL\s""".r
    val FROM$TEMPLATE: Regex = """(?i)\sFROM\s+TEMPLATE\s""".r
    val FROM$DEFAULT$TEMPLATE: Regex = """(?i)\sFROM\s+DEFAULT\s+TEMPLATE$""".r

    def parse(sentence: String, PQL: PQL): Unit = {
        if ($SAVE$AS.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("SAVE", sentence, new SAVE(sentence.takeAfter($SAVE$AS))))
        }
        else {
            throw new SQLParseException("Incorrect SAVE sentence: " + sentence)
        }
    }

    val LINKS: Map[String, Set[String]] =
        Map[String, Set[String]](
            "CACHE" -> Set[String]("TABLE", "PRIMARY$KEY", "PRIMARY"),
            "TEMP" -> Set[String]("TABLE", "PRIMARY$KEY", "PRIMARY", "UNIQUE$KEY", "UNIQUE", "KEY"),
            "TEXT" -> Set[String]("FILE", "WITH$HEADER", "DELIMITED$BY"),
            "CSV" -> Set[String]("FILE", "WITH$HEADER"),
            "JSON" -> Set[String]("FILE")
        )
}

class SAVE(var sentence: String) {

    def execute(PQL: PQL): Unit = {

        val deleteIfExists: Boolean = {
            if (SAVE.NEW.test(sentence)) {
                sentence = sentence.takeAfter(SAVE.NEW).trim()
                true
            }
            else {
                false
            }
        }

        sentence = sentence.$restore(PQL)
//
//        var initial = sentence
//        $BLANK.findFirstIn(sentence) match {
//            case Some(blank) =>
//                initial = sentence.takeBefore(blank)
//                sentence = sentence.takeAfter(blank)
//            case None => sentence = ""
//        }
//
//        initial.trim.toUpperCase match {
//            case "CACHE" =>
//                if (sentence == "") {
//                    PQL.dh.saveAsCache()
//                }
//                else {
//
//                }
//            case "TEMP" =>
//                if (sentence == "") {
//
//                }
//                else {
//
//                }
//            case "EXCEL" =>
//            case "CSV" =>
//            case "TEXT" | "TXT" =>
//            case "DEFAULT" =>
//            case "QROSS" =>
//            case _ =>
//        }

        val sections = sentence.split(BLANKS)

        sections(0).toUpperCase() match {
            case "CACHE" =>
                if (sections.length == 1) {
                    PQL.dh.saveAsCache()
                }
                else if (sections.length == 3) {
                    if (sections(1).equalsIgnoreCase("TABLE")) {
                        PQL.dh.cache(sections(2))
                    }
                    else {
                        throw new SQLParseException("Wrong type name at SAVE AS CACHE: " + sections(1))
                    }
                }
                else if (sections.length > 3) {
                    PRIMARY$KEY.findFirstIn(sentence) match {
                        case Some(primaryKey) =>
                            val key = sentence.takeAfter(primaryKey).trim()
                            if (!key.contains(",") || !$BLANK.test(key)) {
                                PQL.dh.cache(sections(2), key)
                            }
                            else {
                                throw new SQLParseException("Wrong primary key name at SAVE AS CACHE TABLE: " + key)
                            }
                        case None => throw new SQLParseException("Unrecognized sentence at SAVE AS CACHE TABLE: " + sentence.takeAfter(sections(2)))
                    }
                }
                else {
                    throw new SQLParseException("Empty CACHE TABLE name at SAVE AS.")
                }
            case "TEMP" =>
                if (sections.length == 1) {
                    PQL.dh.saveAsTemp()
                }
                else if (sections.length == 3) {
                    if (sections(1).equalsIgnoreCase("TABLE")) {
                        if (sections(1).equalsIgnoreCase("TABLE")) {
                            PQL.dh.temp(sections(2).$eval(PQL).asText)
                        }
                        else {
                            throw new SQLParseException("Wrong TEMP type at SAVE AS: " + sections(1))
                        }
                    }
                }
                else if (sections.length > 3) {
                    sentence = sentence.takeAfter(sections(2))
                    PRIMARY$KEY.findFirstIn(sentence) match {
                        case Some(primaryKey) =>
                            var key = sentence.takeAfter(PRIMARY$KEY).trim()
                            $BLANK.findFirstIn(key) match {
                                case Some(blank) => key = key.takeBefore(blank)
                                case None =>
                            }
                            //sentence = sentence.takeBefore()
                        case None =>
                    }
                }
                else {
                    throw new SQLParseException("Empty TEMP TABLE name at SAVE AS.")
                }
            case "CSV" =>
                if (sections.length > 2 && sections(1).equalsIgnoreCase("FILE")) {
                    sentence = sentence.takeAfter(SAVE.CSV$FILE).trim()
                    var file = ""
                    var header = "NONE"
                    if (SAVE.WITH$HEADER.test(sentence)) {
                        file = sentence.takeBefore(SAVE.WITH$HEADER).trim()
                        if (BLANKS.r.test(file)) {
                            file = file.takeBefore(BLANKS.r)
                        }
                        header = sentence.takeAfter(SAVE.WITH$HEADER).trim()
                    }
                    else if (SAVE.WITH$HEADER$.test(sentence)) {
                        file = sentence.takeBefore(SAVE.WITH$HEADER$).trim()
                        if (BLANKS.r.test(file)) {
                            file = file.takeBefore(BLANKS.r)
                        }
                        header = "HEADER"
                    }
                    else {
                        file = sentence
                    }

                    file = file.$eval(PQL).asText

                    if (file.isFile) {
                        if (deleteIfExists) {
                            PQL.dh.saveAsNewCsvFile(file)
                        }
                        else {
                            PQL.dh.saveAsCsvFile(file)
                        }
                        header match {
                            case "NONE" =>
                            case "HEADER" => PQL.dh.withHeader()
                            case _ => PQL.dh.withHeader(header.$restore(PQL, "\""))
                        }
                        PQL.dh.write()
                    }
                    else {
                        throw new SQLParseException("Incorrect csv file name or path at SAVE AS: " + file)
                    }
                }
                else {
                    throw new SQLParseException("Incorrect sentence SAVE AS CSV FILE or miss arguments.")
                }
            case "TXT" | "TEXT" =>
                if (sections.length > 2 && sections(1).equalsIgnoreCase("FILE")) {
                    sentence = sentence.takeAfter(SAVE.TXT$FILE).trim() + " "
                    var file = ""
                    var header = "NONE"
                    var delimiter = ","

                    /*
                    SAVE AS TXT FILE "file.log"
                        WITH HEADER
                        WITH HEADER { "name": "label" }
                        DELIMITED BY ",";
                    */
                    // DELIMITED BY "," WITH HEADER
                    // DELIMITED BY "," WITH HEADER { "a": "b" }
                    // WITH HEADER DELIMITED BY "";
                    // WITH HEADER { "a": "b" } DELIMITED BY ",";
                    // DELIMITED BY "";
                    // WITH HEADER;
                    // WITH HEADER { };
                    (SAVE.WITH$HEADER.findFirstIn(sentence),
                        SAVE.DELIMITED$BY.findFirstIn(sentence)) match {
                        case (Some(h), Some(d)) =>
                            //header 和 delimiter 都有
                            val i = sentence.indexOf(h)
                            val j = sentence.indexOf(d)
                            if (i < j) {
                                // WITH HEADER 在前
                                file = sentence.takeBefore(h).trim()
                                header = sentence.takeBetween(h, d).trim()
                                delimiter = sentence.takeAfter(d).trim()
                            }
                            else if (j < i) {
                                // DELIMITED BY 在前
                                file = sentence.takeBefore(d).trim()
                                delimiter = sentence.takeBetween(d, h).trim()
                                header = sentence.takeAfter(h).trim()
                            }
                        case (Some(h), None) =>
                            //只有header
                            file = sentence.takeBefore(h).trim()
                            header = sentence.takeAfter(h).trim()
                        case (None, Some(d)) =>
                            //只有delimiter
                            file = sentence.takeBefore(d).trim()
                            delimiter = sentence.takeAfter(d).trim()
                        case (None, None) =>
                            //两个都没有
                            file = sentence.trim()
                    }

                    if (BLANKS.r.test(file)) {
                        file = file.takeBefore(BLANKS)
                    }
                    if (header == "") {
                        header = "HEADER"
                    }

                    file = file.$eval(PQL).asText
                    delimiter = delimiter.$eval(PQL).asText(",")

                    if (file.isFile) {
                        if (deleteIfExists) {
                            PQL.dh.saveAsNewTextFile(file, delimiter)
                        }
                        else {
                            PQL.dh.saveAsTextFile(file, delimiter)
                        }
                        header match {
                            case "NONE" =>
                            case "HEADER" => PQL.dh.withHeader()
                            case _ => PQL.dh.withHeader(header.$restore(PQL, "\""))
                        }
                        PQL.dh.write()
                    }
                    else {
                        throw new SQLParseException("Incorrect text file name or path at SAVE AS: " + file)
                    }
                }
                else {
                    throw new SQLParseException("Incorrect sentence SAVE AS TXT FILE or miss arguments.")
                }
            case "JSON" =>
                if (sections.length > 2 && sections(1).equalsIgnoreCase("FILE")) {
                    var file = sentence.takeAfter(SAVE.JSON$FILE).trim()
                    if (BLANKS.r.test(file)) {
                        file = file.takeBefore(BLANKS.r)
                    }

                    file = file.$eval(PQL).asText

                    if (file.isFile) {
                        if (deleteIfExists) {
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
                }
                else {
                    throw new SQLParseException("Incorrect sentence SAVE AS JSON FILE or miss arguments.")
                }
            case "EXCEL" =>
                if (sections.length >= 2) {
                    var template = ""
                    if (SAVE.FROM$TEMPLATE.test(sentence)) {
                        template = sentence.takeAfter(SAVE.FROM$TEMPLATE).trim()
                    }
                    else if (SAVE.FROM$DEFAULT$TEMPLATE.test(sentence)) {

                    }
                }
                else {
                    throw new SQLParseException("Incorrect sentence SAVE AS EXCEL or miss arguments.")
                }
            case "DEFAULT" =>
                PQL.dh.saveAsDefault()
            case "QROSS" =>
                PQL.dh.saveAsQross()
            case _ =>
                val connectionName =
                    if ($RESERVED.test(sections(0))) {
                        if (!Properties.contains(sections(0))) {
                            throw new SQLExecuteException("Wrong connection name: " + sections(0))
                        }
                        sections(0)
                    }
                    else {
                        sections(0).$eval(PQL).asText
                    }

                if (sections.length > 2 && sections(1).equalsIgnoreCase("USE")) {
                    PQL.dh.saveAs(connectionName, sections(2).$eval(PQL).asText)
                }
                else {
                    PQL.dh.saveAs(connectionName)
                }
        }
    }
}