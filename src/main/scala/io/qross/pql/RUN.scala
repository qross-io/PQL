package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.$RUN
import io.qross.script.Shell._

object RUN {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($RUN.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("RUN", sentence, new RUN(sentence.takeAfter($RUN).trim())))
        }
        else {
            throw new SQLParseException("Incorrect RUN COMMAND/SHELL sentence: " + sentence)
        }
    }
}

class RUN(val commandText: String) {
    def execute(PQL: PQL): Unit = {
        val command = this.commandText.$restore(PQL).removeQuotes()

        PQL.WORKING += command.run()

        if (PQL.dh.debugging) {
            print("RUN SHELL: ")
            println(command)
            println(" EXIT CODE: " + PQL.WORKING.last)
        }
    }
}
