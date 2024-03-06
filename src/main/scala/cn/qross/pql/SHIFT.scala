package cn.qross.pql

import cn.qross.core.DataType
import cn.qross.exception.SQLParseException
import cn.qross.ext.TypeExt._
import cn.qross.pql.Patterns.$SHIFT
import cn.qross.setting.Global

object SHIFT {
    def parse(sentence: String, PQL: PQL): Unit = {
        $SHIFT.findFirstIn(sentence) match {
            case Some(shift) => PQL.PARSING.head.addStatement(new Statement("SHIFT", sentence, new SHIFT(sentence.takeAfter(shift).trim())))
            case None => throw new SQLParseException("Incorrect SHIFT sentence: " + sentence)
        }
    }
}

class SHIFT(val sentence: String) {

    def execute(PQL: PQL): Unit = {
        val table = new Sharp(sentence, PQL.dh.getData.toDataCell(DataType.TABLE))
                        .execute(PQL).asTable

        if (PQL.dh.debugging) {
            table.show(10)
        }

        PQL.dh.clear().buffer(table)
    }
}
