package io.qross.sql

import java.util.regex.{Matcher, Pattern}

import io.qross.core.{DataCell, DataType}
import io.qross.ext.TypeExt._

class Condition(val expression: String) {
    var field: String = null
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

    private val $OPERATOR = Pattern.compile("""==|!=|<>|>=|<=|\^=|=\^|\$=|=\$|\*=|=\*|#=|=#|>|<|=|\sNOT\sIN\s|\sIS\s+NOT\s|^IS\s+NOT\s|\sIN\s|\sIS\s|^IS\s|\sAND\s|\sOR\s|\sNOT\s+EXISTS|^NOT\s+EXISTS\s|\sEXISTS\s|^EXISTS\s|^NOT\s|\sNOT\s""", Pattern.CASE_INSENSITIVE)

    private val m: Matcher = $OPERATOR.matcher(expression)
    if (m.find) {
        this.field = expression.takeBefore(m.group(0)).trim
        this.value = expression.takeAfter(m.group(0)).trim
        this.operator = m.group(0).trim.toUpperCase.replaceAll("""\s+""", "$")
    }


    def eval(field: DataCell, value: DataCell): Unit = {
        this.result = this.operator match {
            case "AND" => field.asBoolean && value.asBoolean
            case "OR" => field.asBoolean || value.asBoolean
            case "NOT" => !value.asBoolean
            case "EXISTS" => value.asText.$trim("(", ")").trim() != ""
            case "NOT$EXISTS" => value.asText.$trim("(", ")").trim() == ""
            case "IN" => value.asScalaList.toSet.contains(field.value)
            case "NOT$IN" => !value.asScalaList.toSet.contains(field.value)
            case "IS" =>
                if (value.isNull) {
                    field.isNull || field.asText.bracketsWith("#{", "}") || field.asText.bracketsWith("&{", "}")
                }
                else if (value.isEmpty) {
                    field.asText == ""
                }
                else {
                    false
                }

            case "IS$NOT" =>
                if (value.isNull) {
                    field.isNotNull && !field.asText.bracketsWith("#{", "}") && !field.asText.bracketsWith("&{", "}")
                }
                else if (value.isEmpty) {
                    field.asText != ""
                }
                else {
                    false
                }
            case "=" =>
            case "==" => field.asText.equalsIgnoreCase(value.asText)
            case "!=" =>
            case "<>" => !field.asText.equalsIgnoreCase(value.asText)
            case "^=" => field.asText.toLowerCase.startsWith(value.asText.toLowerCase)
            case "=^" => !field.asText.toLowerCase.startsWith(value.asText.toLowerCase)
            case "$=" => field.asText.toLowerCase.endsWith(value.asText.toLowerCase)
            case "=$" => !field.asText.toLowerCase.endsWith(value.asText.toLowerCase)
            case "*=" => field.asText.toLowerCase.contains(value.asText.toLowerCase)
            case "=*" => !field.asText.toLowerCase.contains(value.asText.toLowerCase)
            case "#=" => Pattern.compile(value.asText, Pattern.CASE_INSENSITIVE).matcher(field.asText).find
            case "=#" => !Pattern.compile(value.asText, Pattern.CASE_INSENSITIVE).matcher(field.asText).find
            case ">=" =>
                try
                    field.asDecimal >= value.asDecimal
                catch {
                    case e: Exception =>
                        throw new SQLParseException("Value must be number on >= compare: " + field + " >= " + value)
                }
            case "<=" =>
                try
                    field.asDecimal <= value.asDecimal
                catch {
                    case e: Exception =>
                        throw new SQLParseException("Value must be number on >= compare: " + field + " <= " + value)
                }
            case ">" =>
                try
                    field.asDecimal > value.asDecimal
                catch {
                    case e: Exception =>
                        throw new SQLParseException("Value must be number on >= compare: " + field + " > " + value)
                }
            case "<" =>
                try
                    field.asDecimal < value.asDecimal
                catch {
                    case e: Exception =>
                        throw new SQLParseException("Value must be number on >= compare: " + field + " < " + value)
                }
            case _ => value.asBoolean
        }
    }
}
