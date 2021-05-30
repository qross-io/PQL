package io.qross.pql

import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.$ECHO
import io.qross.pql.Solver._

object ECHO {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("ECHO", sentence, new ECHO(sentence.takeAfterX($ECHO))))
    }
}

class ECHO(val content: String) {

    def execute(PQL: PQL): Unit = {
        if (content.nonEmpty) {
            //ECHO不能支持变量$n，js表达式，函数或查询表达式，因为会与js中的定义冲突
            //只支持Sharp表达式，如 ${abc} 但可能与js富字符串冲突
            PQL.RESULT += content.replaceAll("^\\s", "")
                            .replaceEmbeddedVariables(PQL)
                            .replace("~u0025", "%")
        }
    }
}
