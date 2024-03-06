package cn.qross.pql

import cn.qross.core.{DataCell, DataType}
import cn.qross.exception.SQLParseException
import cn.qross.pql.Patterns._
import cn.qross.pql.Solver._
import cn.qross.ext.TypeExt._

import scala.collection.mutable

/*
VAR $m := 1, $n := 2;
VAR $m, $n := 2;
VAR $m := 1, $n;
VAR $m := 2, $n := SELECT $y, $z FROM table1 WHERE a In ($a, $b), $x;
VAR $m := 2, $n := SELECT $y, $z FROM table1 WHERE a In ($a, $b), $x := 'b';
VAR $m := 2, $n := SELECT $y, $z FROM table1 WHERE a In ($a, $b);
*/

object VAR {

    def parse(sentence: String, PQL: PQL): Unit = {
        $VAR.findFirstIn(sentence) match {
            case Some(_) =>
                PQL.PARSING.head.addStatement(
                    new Statement("VAR", sentence, new VAR(sentence.trim().takeAfterX($BLANK).trim()))
                )
            case None => throw new SQLParseException("Incorrect VAR sentence: " + sentence)
        }
    }
}

class VAR(val assignments: String) {

    val variables: mutable.LinkedHashMap[String, String] = new mutable.LinkedHashMap[String, String]()

    private var assigns = "," + assignments
    while (assigns != "") {
        """,\s*\$\w+$""".r.findFirstIn(assigns) match {
            case Some(v) =>
                variables += v.takeAfter(",").trim().toLowerCase() -> null
                assigns = assigns.takeBeforeLast(v).trim()
            case None =>
                """,\s*\$\w+\s*:=""".r.findAllIn(assigns).toList.lastOption match  {
                    case Some(v) =>
                        variables += v.takeBetween(",", ":=").trim().toLowerCase() -> assigns.takeAfterLast(v).trim()
                        assigns = assigns.takeBeforeLast(v).trim()
                    case None =>
                        throw new SQLParseException("Wrong assignment sentence: " + assigns.substring(1))
                }
        }
    }

    def execute(PQL: PQL): Unit = {

        variables.keys.toArray.reverse.foreach(name => {
            val value = variables(name)
            PQL.updateVariable(name, {
                if (value == null) {
                    DataCell.NULL
                }
                else {
                    value.$compute(PQL)
                }
            })
        })
    }
}