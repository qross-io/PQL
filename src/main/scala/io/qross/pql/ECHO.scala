package io.qross.pql

import io.qross.pql.Patterns.{$BLANK, $ECHO}
import io.qross.ext.TypeExt._
import io.qross.pql.Solver._

object ECHO {
    def parse(sentence: String, PQL: PQL): Unit = {
        if($ECHO.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("ECHO", sentence,
                new ECHO({
                    $BLANK.findFirstIn(sentence) match {
                        case Some(blank) => sentence.takeAfter(blank)
                        case None => ""
                    }
                })))
        }
        else {
            throw new SQLParseException("Incorrect ECHO sentence: " + sentence)
        }
    }
}

class ECHO(val content: String) {

    def execute(PQL: PQL): Unit = {
        if (content.nonEmpty) {
            //ECHO不能支持变量，js表达式，函数或查询表达式，因为会与js中的定义冲突
            PQL.RESULT += content.replaceSharpExpressions(PQL).popStash(PQL, "")
        }
    }
}
