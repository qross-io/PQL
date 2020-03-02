package io.qross.pql

import io.qross.pql.Patterns.{$END$IF, $END$LOOP, $END$CASE}
import io.qross.ext.TypeExt._

object END {

    def parse(sentence: String, PQL: PQL): Unit = {
        if ($END$IF.test(sentence)) {
            //检查IF语句是否正常闭合
            if (PQL.TO_BE_CLOSE.isEmpty) {
                throw new SQLParseException("Can't find IF clause: " + sentence)
            }
            else if (PQL.TO_BE_CLOSE.head.caption != "IF") {
                throw new SQLParseException(PQL.TO_BE_CLOSE.head.caption + " hasn't closed: " + PQL.TO_BE_CLOSE.head.sentence)
            }
            else {
                PQL.TO_BE_CLOSE.pop()
            }

            val $endIf: Statement = new Statement("END$IF", "END IF", new END$IF())
            //只出栈
            PQL.PARSING.pop()
            PQL.PARSING.head.addStatement($endIf)
        }
        else if ($END$LOOP.test(sentence)) {
            //检查FOR语句是否正常闭合
            if (PQL.TO_BE_CLOSE.isEmpty) {
                throw new SQLParseException("Can't find FOR or WHILE clause: " + sentence)
            }
            else if (!Set("FOR" , "WHILE").contains(PQL.TO_BE_CLOSE.head.caption)) {
                throw new SQLParseException(PQL.TO_BE_CLOSE.head.caption + " hasn't closed: " + PQL.TO_BE_CLOSE.head.sentence)
            }
            else {
                PQL.TO_BE_CLOSE.pop()
            }

            val $endLoop: Statement = new Statement("END$LOOP", "END LOOP", new END$LOOP())
            //只出栈
            PQL.PARSING.pop()
            PQL.PARSING.head.addStatement($endLoop)
        }
        else if ($END$CASE.test(sentence)) {
            //检查CASE WHEN语句是否正常闭合
            if (PQL.TO_BE_CLOSE.isEmpty) {
                throw new SQLParseException("Can't find CASE clause: " + sentence)
            }
            else if (PQL.TO_BE_CLOSE.head.caption != "CASE") {
                throw new SQLParseException(PQL.TO_BE_CLOSE.head.caption + " hasn't closed: " + PQL.TO_BE_CLOSE.head.sentence)
            }
            else {
                PQL.TO_BE_CLOSE.pop()
            }

            val $endCase: Statement = new Statement("END$CASE", "END CASE", new END$CASE())
            //只出栈
            PQL.PARSING.pop() //退出WHEN或ELSE
            PQL.PARSING.pop() //退出CASE
            PQL.PARSING.head.addStatement($endCase)
        }
        else if ("END".equalsIgnoreCase(sentence)) {
            //END FUNCTION
            if (PQL.TO_BE_CLOSE.isEmpty) {
                throw new SQLParseException("Can't find FUNCTION clause: " + sentence)
            }
            else if (PQL.TO_BE_CLOSE.head.caption != "FUNCTION") {
                throw new SQLParseException(PQL.TO_BE_CLOSE.head.caption + " hasn't closed: " + PQL.TO_BE_CLOSE.head.sentence)
            }
            else {
                PQL.TO_BE_CLOSE.pop()
            }

            val $end: Statement = new Statement("END", "END", new END())
            //END是FUNCTION的最后一条子语句
            PQL.PARSING.head.addStatement($end)

            //将语句从当前节点转移到functions
            val $function = PQL.PARSING.pop()
            PQL.USER$FUNCTIONS += $function.instance.asInstanceOf[FUNCTION].functionName -> new UserFunction($function)
        }
        else {
            throw new SQLParseException("Incorrect END sentence: " + sentence)
        }
    }
}

class END {
    def execute(PQL: PQL): Unit = {
        //退出CALL FUNCTION
        PQL.EXECUTING.pop()
    }
}
