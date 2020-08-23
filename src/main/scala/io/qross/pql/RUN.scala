package io.qross.pql

import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.$RUN
import io.qross.pql.Solver._
import io.qross.script.Shell._

object RUN {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("RUN", sentence, new RUN(sentence.takeAfterX($RUN).trim())))
    }
}

class RUN(val commandText: String) {
    def execute(PQL: PQL): Unit = {
        val command = this.commandText.$restore(PQL).removeQuotes()
        val exitCode = command.run()

        if (PQL.dh.debugging) {
            print("RUN SHELL: ")
            println(command)
            println("EXIT CODE: " + exitCode)
        }
    }
}
