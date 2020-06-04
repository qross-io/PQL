package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.$RUN

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

//SHELL命令中 引号会出现问题，复杂的shell建议生成sh文件

class RUN(val commandText: String) {
    def execute(PQL: PQL): Unit = {
        val command = this.commandText.$restore(PQL).removeQuotes()

        if (PQL.dh.debugging) {
            print("RUN SHELL: ")
            println(command)
        }

        PQL.RESULT += command.bash()
    }
}
