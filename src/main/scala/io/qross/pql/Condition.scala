package io.qross.pql

import java.util.regex.{Matcher, Pattern}

import io.qross.core.{DataCell, DataType}
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.BLANKS

import scala.collection.mutable.ListBuffer

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

    private val $OPERATOR = """(?i)===|!==|==|!=|<>|>=|<=|>|<|=|\sNOT\s+IN\b|\sIS\s+NOT\s|\sIN\s|\sIS\s|\sAND\s|\sOR\s|\sNOT\s+EXISTS\b|\sEXISTS\b|\sNOT\s""".r

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
            case "IN" =>
                if (value.isText) {
                    val chars = new ListBuffer[String]
                    value.asText
                            .pickChars(chars)
                            .$trim("(", ")")
                            .split(",")
                            .toSet[String]
                            .map(_.restoreChars(chars).removeQuotes())
                            .contains(field.asText.removeQuotes())
                }
                else {
                    value.asList.toSet.contains(field.value)
                }
            case "NOT$IN" =>
                if (value.isText) {
                    val chars = new ListBuffer[String]
                    !value.asText
                            .pickChars(chars)
                            .$trim("(", ")")
                            .split(",")
                            .toSet[String]
                            .map(_.restoreChars(chars).removeQuotes())
                            .contains(field.asText.removeQuotes())
                }
                else {
                    !value.asList.toSet.contains(field.value)
                }
            case "IS$NOT" =>
                val v = value.asText
                if (v == null) {
                    field.value != null
                }
                else if ("""(?i)^[a-z]+$""".r.test(v)) {
                    v.toUpperCase match {
                        case "NULL" => field.value != null
                        case "EMPTY" => field.nonEmpty
                        case "DEFINED" =>
                            if (field.dataType == DataType.EXCEPTION && field.value == "NOT_FOUND") {
                                true
                            }
                            else if (Solver.ARGUMENT.test(field.asText.removeQuotes())) {
                                true
                            }
                            else {
                                false
                            }
                        case "UNDEFINED" =>
                            if (field.dataType == DataType.EXCEPTION && field.value == "NOT_FOUND") {
                                false
                            }
                            else if (Solver.ARGUMENT.test(field.asText.removeQuotes())) {
                                false
                            }
                            else {
                                true
                            }
                        case "TABLE" | "DATATABLE" => !field.isTable
                        case "ROW" | "DATAROW" | "MAP" | "OBJECT" => !field.isRow
                        case "LIST" | "ARRAY" => !field.isJavaList
                        case "STRING" | "TEXT" => !field.isText
                        case "DECIMAL" | "NUMERIC" => !field.isDecimal
                        case "INT" | "INTEGER" => !field.isInteger
                        case "BOOL" | "BOOLEAN" => !field.isBoolean
                        case "DATETIME" => !field.isDateTime
                        case _ => field.dataType.typeName != v
                    }
                }
                else {
                    field.value == value.value
                }
            case "IS" =>
                val v = value.asText
                if (v == null) {
                    field.value == null
                }
                else if ("""(?i)^[a-z]+$""".r.test(v)) {
                    v.toUpperCase match {
                        case "NULL" => field.value == null
                        case "EMPTY" => field.isEmpty
                        case "DEFINED" =>
                            if (field.dataType == DataType.EXCEPTION && field.value == "NOT_FOUND") {
                                false
                            }
                            else if (Solver.ARGUMENT.test(field.asText.removeQuotes())) {
                                false
                            }
                            else {
                                true
                            }
                        case "UNDEFINED" =>
                            if (field.dataType == DataType.EXCEPTION && field.value == "NOT_FOUND") {
                                true
                            }
                            else if (Solver.ARGUMENT.test(field.asText.removeQuotes())) {
                                true
                            }
                            else {
                                false
                            }
                        case "TABLE" | "DATATABLE" => field.isTable
                        case "ROW" | "DATAROW" | "MAP" | "OBJECT" => field.isRow
                        case "LIST" | "ARRAY" => field.isJavaList
                        case "STRING" | "TEXT" => field.isText
                        case "DECIMAL" | "NUMERIC" => field.isDecimal
                        case "INT" | "INTEGER" => field.isInteger
                        case "BOOL" | "BOOLEAN" => field.isBoolean
                        case "DATETIME" => field.isDateTime
                        case _ => field.dataType.typeName == v
                    }
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
