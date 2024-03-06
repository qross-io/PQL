package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.ext.TypeExt._
import cn.qross.pql.Patterns.$TRANS
import cn.qross.pql.Solver._

object TRANS {
    def parse(sentence: String, PQL: PQL): Unit = {
        $TRANS.findFirstIn(sentence) match {
            case Some(trans) => PQL.PARSING.head.addStatement(new Statement("TRANS", sentence, new TRANS(sentence.takeAfter(trans).trim())))
            case None => throw new SQLParseException("Incorrect TRANS sentence: " + sentence)
        }
    }
}

class TRANS(val sentence: String) {

    def execute(PQL: PQL): Unit = {
        PQL.dh.trans(sentence.restoreJsons(PQL).$restore(PQL))
    }
}

