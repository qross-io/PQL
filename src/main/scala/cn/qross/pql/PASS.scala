package cn.qross.pql

import cn.qross.exception.{SQLExecuteException, SQLParseException}
import cn.qross.ext.TypeExt._
import cn.qross.pql.Patterns.$PASS
import cn.qross.pql.Solver._

object PASS {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($PASS.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("PASS", sentence, new PASS(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect PASS sentence: " + sentence)
        }
    }
}

class PASS(val sentence: String) {
    def execute(PQL: PQL): Unit = {

        val data =  {
            sentence.takeBeforeX(Patterns.$BLANK).toUpperCase() match {
                case "SELECT" => new SELECT(sentence).select(PQL, PQL.dh.getData) //PQL.dh.pass(sentence.$restore(PQL)
                case "REDIS" => new REDIS(sentence).evaluate(PQL, PQL.dh.getData) //PQL.dh.transfer(sentence.$restore(PQL))
                case "" => throw new SQLExecuteException("Incomplete or empty PASS sentence: " + sentence)
                case _ => throw new SQLExecuteException("Unsupported PASS sentence: " + sentence)
            }
        }

        //sentence.$compute(PQL).asTable
        PQL.dh.clear().buffer(data.asTable)
    }
}
