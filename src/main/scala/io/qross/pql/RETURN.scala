package io.qross.pql

import io.qross.core.DataCell
import io.qross.exception.SQLParseException
import io.qross.ext.Output
import io.qross.pql.Patterns.$RETURN
import io.qross.ext.TypeExt._

object RETURN {
    def parse(sentence: String, PQL: PQL): Unit = {
        if($RETURN.test(sentence)) {
            val content = sentence.takeAfter($RETURN)
            PQL.PARSING.head.addStatement(new Statement("RETURN", sentence, new RETURN(content)))
        }
        else {
            throw new SQLParseException("Incorrect RETURN sentence: " + sentence)
        }
    }
}

class RETURN(content: String) {

    //执行但不返回数据, 返回数据只出现在嵌入式场景下
    def execute(PQL: PQL): Unit = {
        //退出直到碰到CALL语句或ROOT
        while (PQL.EXECUTING.head.caption != "CALL" && PQL.EXECUTING.head.caption != "ROOT") {
            PQL.EXECUTING.pop()
        }

        if (PQL.EXECUTING.head.caption == "CALL") {
            PQL.EXECUTING.pop()
        }
        else if (PQL.EXECUTING.head.caption == "ROOT") {
            if (PQL.dh.debugging) {
                Output.writeDebugging(s"System exit use 'RETURN'.")
            }
            System.exit(0)
        }
    }
}
