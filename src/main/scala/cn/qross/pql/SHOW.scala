package cn.qross.pql

import cn.qross.core.DataTable
import cn.qross.exception.SQLParseException
import cn.qross.pql.Patterns.{$SHOW, $BLANK}
import cn.qross.pql.Solver._
import cn.qross.ext.TypeExt._

object SHOW {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("SHOW", sentence, new SHOW(sentence)))
    }
}

class SHOW(val sentence: String) {
    def execute(PQL: PQL): Unit = {
        if ("""(?i)SHOW\s+[a-z]+""".r.test(sentence)) {

            PQL.WORKING += new SELECT(sentence).evaluate(PQL).value

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
