package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns._
import io.qross.pql.Solver._

object INSERT {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($INSERT$INTO.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("INSERT", sentence, new INSERT(sentence)))
        }
        else {
            throw new SQLParseException("Incorrect INSERT INTO sentence: " + sentence)
        }
    }
}

class INSERT(var sentence: String) {

    def insert(PQL: PQL, express: Boolean = false): DataCell = {

        val (_insert, links) =
            if (sentence.contains(ARROW)) {
                (sentence.takeBefore(ARROW), sentence.takeAfter(ARROW))
            }
            else {
                (sentence, "")
            }

        PQL.AFFECTED_ROWS_OF_LAST_NON_QUERY = PQL.dh.set({
            if (express) {
                _insert.$express(PQL).popStash(PQL)
            }
            else {
                _insert.$restore(PQL)
            }
        }).AFFECTED_ROWS_OF_LAST_SET

        val data = DataCell(PQL.AFFECTED_ROWS_OF_LAST_NON_QUERY, DataType.INTEGER)
        if (links != "") {
            new Sharp(links, data).execute(PQL)
        }
        else {
            data
        }
    }

    def execute(PQL: PQL): Unit = {
        PQL.AFFECTED_ROWS_OF_LAST_NON_QUERY = PQL.dh.set(sentence.$restore(PQL)).AFFECTED_ROWS_OF_LAST_SET
    }
}
