package io.qross.pql

import io.qross.core.DataCell
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.$RETURN
import io.qross.pql.Solver._

object RETURN {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("RETURN", sentence, new RETURN(sentence.takeAfterX($RETURN).trim())))
    }
}

class RETURN(content: String) {

    def execute(PQL: PQL): Unit = {
        //因为RETURN可能在控制语句中，所以退出直到碰到CALL语句或ROOT
        while (PQL.EXECUTING.head.caption != "CALL" && PQL.EXECUTING.head.caption != "ROOT") {
            PQL.EXECUTING.pop()
        }

        if (PQL.EXECUTING.head.caption == "CALL") {
            if (content != "") {
                PQL.FUNCTION$RETURNS += content.$eval(PQL)
            }
            else {
                PQL.FUNCTION$RETURNS += DataCell.NULL
            }
            PQL.EXECUTING.pop()
        }
        else if (PQL.EXECUTING.head.caption == "ROOT") {
            if (content != "") {
                PQL.RESULT += content.$compute(PQL).value
            }
            else {
                PQL.RESULT += null
            }
        }
    }
}
