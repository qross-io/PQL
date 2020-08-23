package io.qross.pql

import io.qross.core.DataTable
import io.qross.exception.SQLParseException
import io.qross.pql.Patterns.{$SHOW, $BLANK}
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

object SHOW {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("SHOW", sentence, new SHOW(sentence)))
    }
}

class SHOW(val sentence: String) {
    def execute(PQL: PQL): Unit = {
        if ("""(?i)SHOW\s+[a-z]+""".r.test(sentence)) {

            PQL.WORKING += new SELECT(sentence).select(PQL).value

            """(?i)SHOW\s+CREATE\s+TABLE\s""".r.findFirstIn(sentence) match {
                case Some(_) =>
                    PQL.WORKING.last.asInstanceOf[DataTable].firstRow match {
                        case Some(row) => println(row.getString("Create Table"))
                        case None =>
                    }
                case None =>
            }
        }
        else {
            PQL.dh.show(sentence.takeAfterX($BLANK).$eval(PQL).asInteger(20).toInt)
        }
    }
}
