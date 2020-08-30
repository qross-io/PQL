package io.qross.pql

import io.qross.core.{DataCell, DataRow, DataType}
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.$RUN
import io.qross.pql.Solver._
import io.qross.script.Shell._

object RUN {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("RUN", sentence, new RUN(sentence)))
    }
}

class RUN(val sentence: String) {

    def evaluate(PQL: PQL, express: Int = Solver.FULL): DataCell = {
        sentence.$process(PQL, express, body => {
            DataCell(sentence.takeAfterX($RUN).removeQuotes().run(), DataType.ROW)
        })
    }

    def execute(PQL: PQL): Unit = {
        PQL.WORKING += evaluate(PQL).value.asInstanceOf[DataRow]
    }
}
