package io.qross.sql

import io.qross.sql.Solver._

class FOR$TO(val variable: String, rangeBegin: String, rangeEnd: String) {

    def parseBegin(PSQL: PSQL): Integer = {
        rangeBegin.$eval(PSQL).toInt
    }

    def parseEnd(PSQL: PSQL): Integer = {
        rangeEnd.$eval(PSQL).toInt
    }

    def hasNext(PSQL: PSQL): Boolean = {
        PSQL.findVariable(variable).value.asInstanceOf[Int] <= parseEnd(PSQL)
    }
}
