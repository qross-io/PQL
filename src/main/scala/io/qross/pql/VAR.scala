package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.exception.SQLParseException
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

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
                    new Statement("VAR", sentence, new VAR(sentence.trim().takeAfter($BLANK).trim()))
                )
            case None => throw new SQLParseException("Incorrect VAR sentence: " + sentence)
        }
    }
}

class VAR(val assignments: String) {

    def execute(PQL: PQL): Unit = {

        var assigns = "," + assignments
        val aps = new mutable.LinkedHashMap[String, String]()

        while (assigns != "") {
            """(?i),\s*\$[a-z0-9_]+$""".r.findFirstIn(assigns) match {
                case Some(v) =>
                    aps += v.takeAfter(",").trim() -> null
                    assigns = assigns.takeBeforeLast(v).trim()
                case None =>
                    """(?i),\s*\$[a-z0-9_]+\s*:=""".r.findAllIn(assigns).toList.lastOption match  {
                        case Some(v) =>
                            aps += v.takeBetween(",", ":=").trim() -> assigns.takeAfterLast(v).trim()
                            assigns = assigns.takeBeforeLast(v).trim()
                        case None =>
                            throw new SQLParseException("Wrong assignment sentence: " + assigns.substring(1))
                    }
            }
        }

        aps.keys.toArray.reverse.foreach(name => {
            val value = aps(name)
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