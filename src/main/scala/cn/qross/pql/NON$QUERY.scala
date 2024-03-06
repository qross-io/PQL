package cn.qross.pql

import cn.qross.core.{DataCell, DataType}
import cn.qross.exception.SQLExecuteException
import cn.qross.pql.Solver._

class NON$QUERY(sentence: String) {

    def evaluate(PQL: PQL, express: Int = Solver.FULL): DataCell = {
        sentence.$process(PQL, express, nonQuery => {
            val args = nonQuery.findArguments
            if (args.isEmpty) {
                PQL.AFFECTED_ROWS_OF_LAST_NON_QUERY = PQL.dh.executeNonQuery(nonQuery)
                DataCell(PQL.AFFECTED_ROWS_OF_LAST_NON_QUERY, DataType.INTEGER)
            }
            else {
                throw new SQLExecuteException(s"Missed arguments: ${args.mkString(", ")}")
            }
        })
    }

    def execute(PQL: PQL): Unit = {
        PQL.WORKING += this.evaluate(PQL).value
    }
}
