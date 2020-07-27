package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.pql.Solver._

class NON$QUERY(sentence: String) {

    def affect(PQL: PQL, express: Int = Solver.FULL): DataCell = {
        sentence.$process(PQL, express, nonQuery => {
            PQL.AFFECTED_ROWS_OF_LAST_NON_QUERY = PQL.dh.executeNonQuery(nonQuery)
            DataCell(PQL.AFFECTED_ROWS_OF_LAST_NON_QUERY, DataType.INTEGER)
        })
    }

    def execute(PQL: PQL): Unit = {
        PQL.WORKING += this.affect(PQL).value
    }
}
