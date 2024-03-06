package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.pql.Patterns.$PAGE
import cn.qross.ext.TypeExt._
import cn.qross.pql.Solver._

object PAGE {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($PAGE.test(sentence)) {
            val $page = new Statement("PAGE", sentence.takeBefore("#"), new PAGE(sentence.takeAfter("#").trim))
            PQL.PARSING.head.addStatement($page)
        }
        else {
            throw new SQLParseException("Incorrect PAGE sentence: " + sentence)
        }
    }
}

class PAGE(val selectSQL: String) {
    def execute(PQL: PQL): Unit = {
        PQL.dh.page(this.selectSQL.$restore(PQL))
    }
}
