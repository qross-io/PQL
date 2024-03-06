package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.pql.Patterns.$PREP
import cn.qross.pql.Solver._
import cn.qross.ext.TypeExt._

object PREP {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($PREP.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("PREP", sentence, new PREP(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect PREP sentence: " + sentence)
        }
    }
}

class PREP(val nonQuerySQL: String) {
    def execute(PQL: PQL): Unit = {
        PQL.dh.prep(this.nonQuerySQL.$restore(PQL))
    }
}
