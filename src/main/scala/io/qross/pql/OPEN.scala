package io.qross.pql

import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.{$OPEN, $RESERVED, BLANKS}
import io.qross.setting.Properties
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
        if ($OPEN.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("OPEN", sentence, new OPEN(sentence.takeAfter($OPEN))))
            //            if (m.group(2).trim == ":") {
            //                parseStatement(sentence.takeAfter(m.group(2)).trim)
            //            }
        }
        else {
            throw new SQLParseException("Incorrect OPEN sentence: " + sentence)
        }
    }
}

class OPEN(val sentence: String) {

    def execute(PQL: PQL): Unit = {

        val sections = sentence.split(BLANKS)

        sections(0).toUpperCase() match {
            case "CACHE" => PQL.dh.openCache()
            case "TEMP" => PQL.dh.openTemp()
            case "DEFAULT" => PQL.dh.openDefault()
            case "QROSS" => PQL.dh.openQross()
            case _ =>
                val connectionName = {
                    if ($RESERVED.test(sections(0))) {
                        if (!Properties.contains(sections(0))) {
                            throw new SQLExecuteException("Wrong connection name: " + sections(0))
                        }
                        sections(0)
                    }
                    else {
                        sections(0).$eval(PQL).asText
                    }
                }

                if (sections.length > 2) {
                    if (sections(1).equalsIgnoreCase("USE")) {
                        PQL.dh.open(connectionName, sections(2).$eval(PQL).asText)
                    }
                }
                else {
                    PQL.dh.open(connectionName)
                }
        }
    }
}
