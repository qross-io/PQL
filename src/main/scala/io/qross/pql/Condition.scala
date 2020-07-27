package io.qross.pql

import io.qross.core.{DataCell}
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.BLANKS
import io.qross.pql.Solver._
import scala.collection.mutable.ListBuffer
import io.qross.fs.Path._

class Condition(val expression: String) {
    var field: String = _
    var operator: String = ""
    var value: String = expression //or unary

    var result = false

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

    private val $OPERATOR = """(?i)===|!==|==|!=|<>|>=|<=|>|<|=|\sNOT\s+IN\b|\sIS\s+NOT\s|\sIN\b|\sIS\s|\sAND\s|\sOR\s|\bNOT\s+EXISTS\b|\bEXISTS\b|^NOT\s""".r

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
            case "EXISTS" =>
                val left = field.asText
                if (left == null || left == "") {
                    value.asText.$trim("(", ")").$trim("[", "]").trim() != ""
                }
                else {
                    left.toUpperCase() match {
                        case "FILE" | "DIR" => value.asText.fileExists

                        case _ => false
                    }
                }
            case "NOT$EXISTS" =>
                val left = field.asText
                if (left == null || left == "") {
                    value.asText.$trim("(", ")").$trim("[", "]").trim() == ""
                }
                else {
                    left.toUpperCase() match {
                        case "FILE" | "DIR" => !value.asText.fileExists
                        case _ => true
                    }
                }
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
                    field.valid && field.value != null
                }
                else if ("""(?i)^[a-z]+$""".r.test(v)) {
                    v.toUpperCase match {
                        case "NULL" => field.valid && field.value != null
                        case "EMPTY" => field.nonEmpty
                        case "UNDEFINED" =>
                            if (field.undefined || field.value == "UNDEFINED") {
                                false
                            }
                            else if (field.asText.removeQuotes().containsArguments) {
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
                    field.value != value.value
                }
            case "IS" =>
                val v = value.asText
                if (v == null) {
                    field.invalid || field.value == null
                }
                else if ("""(?i)^[a-z]+$""".r.test(v)) {
                    v.toUpperCase match {
                        case "NULL" => field.invalid || field.value == null
                        case "EMPTY" => field.isEmpty
                        case "DEFINED" =>
                            if (field.undefined || field.value == "UNDEFINED") {
                                false
                            }
                            else if (field.asText.removeQuotes().containsArguments) {
                                false
                            }
                            else {
                                true
                            }
                        case "UNDEFINED" =>
                            if (field.undefined || field.value == "UNDEFINED") {
                                true
                            }
                            else if (field.asText.removeQuotes().containsArguments) {
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
            case "===" => field.dataType == value.dataType && field.value == value.value
            case "!==" => field.dataType != value.dataType || field.value != value.value
            case "=" | "==" =>
                // null == undefined
                {
                    if (field.undefined || field.value == null) {
                        "null"
                    }
                    else {
                        field.asText
                    }
                }.equalsIgnoreCase({
                    if (value.undefined || value.value == null) {
                        "null"
                    }
                    else {
                        value.asText
                    }
                })
                //field.asText.equalsIgnoreCase(value.asText)
            case "!=" | "<>" =>
                !{
                    {
                        if (field.undefined || field.value == null) {
                            "null"
                        }
                        else {
                            field.asText
                        }
                    }.equalsIgnoreCase({
                        if (value.undefined || value.value == null) {
                            "null"
                        }
                        else {
                            value.asText
                        }
                    })
                }
            //!field.asText.equalsIgnoreCase(value.asText)
            case ">=" => field >= value
            case "<=" => field <= value
            case ">" => field > value
            case "<" => field < value
            case _ => value.asBoolean
        }
    }
}
