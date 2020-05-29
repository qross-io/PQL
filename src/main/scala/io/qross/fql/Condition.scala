package io.qross.fql

import io.qross.core.DataCell
import io.qross.ext.TypeExt._
import io.qross.fql.Patterns._
import io.qross.time.DateTime

object Condition {
    def eval(field: Any, operator: String, value: Any): Boolean = {

        operator match {
            case "AND" => field.toBoolean(false) && value.toBoolean(false)
            case "OR" => field.toBoolean(false) || value.toBoolean(false)
            case "NOT" => !value.toBoolean(false)
            case "IN" =>
                field match {
                    case str: String =>
                        str.contains("'" + field + "'") ||
                            str.contains("\"" + field + "\"")
                    case dt: DateTime =>
                        dt.toString().contains("'" + field.toString + "'") ||
                            dt.toString().contains("\"" + field.toString + "\"")
                    case _ =>
                        ("," + value.asInstanceOf[String] + ",").contains(field.toString)
                }
            case "NOT$IN" =>
                field match {
                    case str: String =>
                        !str.contains("'" + field + "'") &&
                            !str.contains("\"" + field + "\"")
                    case dt: DateTime =>
                        !dt.toString().contains("'" + field.toString + "'") &&
                            !dt.toString().contains("\"" + field.toString + "\"")
                    case _ =>
                        !("," + value.asInstanceOf[String] + ",").contains(field.toString)
                }
            case "IS$NOT" => field != value
            case "IS" => field == value
            case "=" => field == value
            case "!=" | "<>" => field.toString != value.toString
            case ">=" => field >= value
            case "<=" => field <= value
            case ">" => field > value
            case "<" => field < value
            case _ => value.toBoolean(false)
        }
    }
}

class Condition(expression: String) {

    var field: String = _
    var operator: String = ""
    var value: String = expression //or unary

    /*
    operator 列表

    等于 =, ==
    不等于 !=, <>
    在列表中 IN ()
    不在列表中 NOT IN ()
    大于 >
    小于 <
    大于等于 >=
    小于等于 <=
    为NULL IS NULL
    不为NULL IS NOT NULL
    为空值 IS EMPTY
    不为空值 IS NOT EMPTY

    AND
    OR
    NOT
    */

    private val $OPERATOR = """(?i)!=|<>|>=|<=|>|<|=|\sNOT\s+IN\b|\sIS\s+NOT\s|\sIN\b|\sIS\s|\sAND\s|\sOR\s|^NOT\s""".r

    $OPERATOR.findFirstIn(expression) match {
        case Some(opt) =>
            this.field = expression.takeBefore(opt).trim()
            this.value = expression.takeAfter(opt).trim()
            this.operator = opt.trim.toUpperCase.replaceAll(BLANKS, "\\$")
        case None =>
    }

    if (operator == "IN" || operator == "NOT$IN") {
        value = value.replace(BLANKS, "").$trim("(", ")")
    }
}
