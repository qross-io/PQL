package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.{$SELECT$CUSTOM, ARROW}
import io.qross.pql.Solver._

object SELECT {

    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("SELECT", sentence, new SELECT(sentence)))
    }
}

class SELECT(val sentence: String) {

    def select(PQL: PQL, express: Boolean = false): DataCell = {

        val (_select, links) =
            if (sentence.contains(ARROW)) {
                (sentence.takeBefore(ARROW), sentence.takeAfter(ARROW))
            }
            else {
                (sentence, "")
            }

        val data = {
            PQL.dh.executeDataTable({
                if (express) {
                    _select.$express(PQL).popStash(PQL)
                }
                else {
                    _select.$restore(PQL)
                }
            }).toDataCell(DataType.TABLE)
        }

        PQL.COUNT_OF_LAST_SELECT = data.dataType match {
            case DataType.TABLE => data.asTable.size
            case DataType.ARRAY | DataType.LIST => data.asList.size
            case _ => 1
        }

        if (links != "") {
            new Sharp(links, data).execute(PQL)
        }
        else {
            data
        }
    }

    def execute(PQL: PQL): Unit = {
        PQL.RESULT += this.select(PQL).value
    }
}