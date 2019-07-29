package io.qross.sql

import io.qross.core.DataRow
import io.qross.ext.TypeExt._
import io.qross.sql.Solver._

import scala.collection.mutable.ArrayBuffer

class FOR$IN(val forItems: String, forCollection: String, val delimiter: String) {

    val fields = new ArrayBuffer[String]()
    val separators = new ArrayBuffer[String]()

    private var declarations = forItems

    USER_VARIABLE
            .findAllMatchIn(forItems)
            .map(m => m.group(2))
            .foreach(field => fields += field)

    for (i <- fields.indices) {
        separators += declarations.takeBefore(fields(i))
        declarations = declarations.takeAfter(fields(i))
    }

    if (!declarations.isEmpty) {
        separators += declarations
    }
    else {
        separators += ""
    }

    def computeMap(PSQL: PSQL): ForVariables = {
        val variablesMaps = new ForVariables()
        val collection = this.forCollection.$eval(PSQL).split(delimiter, -1)
        for (i <- collection.indices) {
            var line = collection(i)
            val row = new DataRow()
            for (j <- fields.indices) {
                val field = fields(j).substring(1).toUpperCase
                val prefix = separators(j)
                val suffix = separators(j + 1)

                line = line.substring(prefix.length)
                if (suffix.isEmpty) {
                    row.set(field, line)
                    //line = ""; //set empty
                }
                else {
                    row.set(field, line.substring(0, line.indexOf(suffix)))
                    line = line.substring(line.indexOf(suffix))
                }
            }
            variablesMaps.addRow(row)
        }
        variablesMaps
    }
}
