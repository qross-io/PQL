package io.qross.pql

import java.util.regex.{Matcher, Pattern}

import io.qross.core.DataCell
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.BLANKS

class Condition(val expression: String) {
    var field: String = _
    var operator: String = ""
    var value: String = expression //or unary

    var result = false

    /*
    operator 列表

    等于 =, ==
    不等于 !=, <>
    开始于 ^=
    非开始于 =^
    结束于 $=
    非结束于 =$
    包含于 *=
    不包含于 =*
    正则表达式匹配 #=
    正则表达式不匹配 =#
    存在 EXISTS ()
    不存在 NOT EXISTS ()
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

    private val $OPERATOR = """(?i)===|!==|==|!=|<>|>=|<=|>|<|=|\sNOT\sIN\s|\sIS\s+NOT\s|^IS\s+NOT\s|\sIN\s|\sIS\s|^IS\s|\sAND\s|\sOR\s|\sNOT\s+EXISTS|^NOT\s+EXISTS\s|\sEXISTS\s|^EXISTS\s|\sLIKE\s|\sNOT\sLIKE\s|^NOT\s|\sNOT\s""".r

    $OPERATOR.findFirstIn(expression) match {
        case Some(opt) =>
            this.field = expression.takeBefore(opt).trim
            this.value = expression.takeAfter(opt).trim
            this.operator = opt.trim.toUpperCase.replaceAll(BLANKS, "\\$")
        case None =>
    }

    def eval(field: DataCell, value: DataCell): Unit = {
        this.result = this.operator match {
            case "AND" => field.asBoolean && value.asBoolean
            case "OR" => field.asBoolean || value.asBoolean
            case "NOT" => !value.asBoolean
            case "EXISTS" => value.asText.$trim("(", ")").$trim("[", "]").trim() != ""
            case "NOT$EXISTS" => value.asText.$trim("(", ")").$trim("[", "]").trim() == ""
            case "IN" => value.asList.toSet.contains(field.value)
            case "NOT$IN" => !value.asList.toSet.contains(field.value)
            case "IS$NOT" =>
                val v = value.asText
                if (v == null || v.equalsIgnoreCase("NULL")) {
                    field.value != null
                }
                else if (v.equalsIgnoreCase("EMPTY")) {
                    field.nonEmpty
                }
                else {
                    field.value != value.value
                }
            case "IS" =>
                val v = value.asText
                if (v == null || v.equalsIgnoreCase("NULL")) {
                    field.value == null
                }
                else if (value.asText.equalsIgnoreCase("EMPTY")) {
                    field.isEmpty
                }
                else {
                    field.value == value.value
                }
            case "===" => field.asText == value.asText
            case "!==" => field.asText != value.asText
            case "=" | "==" => field.asText.equalsIgnoreCase(value.asText)
            case "!=" | "<>" => !field.asText.equalsIgnoreCase(value.asText)
            case ">=" => field >= value
            case "<=" => field <= value
            case ">" => field > value
            case "<" => field < value
            case _ => value.asBoolean
        }
    }
}
