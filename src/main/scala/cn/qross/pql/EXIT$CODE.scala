package cn.qross.pql

import cn.qross.ext.Output
import cn.qross.pql.Solver._

class EXIT$CODE(code: String) {
    def execute(PQL: PQL): Unit = {
        val exitCode = if (code == "") 0 else code.$eval(PQL).asInteger(0).toInt
        if (PQL.dh.debugging) {
            Output.writeDebugging(s"System exit with code $exitCode.")
        }
        System.exit(exitCode)
    }
}
