package cn.qross.pql

import cn.qross.core.DataCell
import cn.qross.ext.Output
import cn.qross.ext.TypeExt._
import cn.qross.pql.Patterns.$RETURN
import cn.qross.pql.Solver._

object RETURN {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("RETURN", sentence, new RETURN(sentence.takeAfterX($RETURN).trim())))
    }
}

class RETURN(content: String) {

    def execute(PQL: PQL): Unit = {
        //因为 RETURN 可能在控制语句中，所以退出直到冒泡到 CALL 语句或 ROOT
        while (PQL.EXECUTING.nonEmpty && PQL.EXECUTING.head.caption != "CALL" && PQL.EXECUTING.head.caption != "ROOT") {
            PQL.EXECUTING.pop()
        }

        if (PQL.EXECUTING.nonEmpty) {
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
                //不再执行后面的所有语句
                PQL.BREAK = true
            }
        }
    }
}
