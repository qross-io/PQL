package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.{$CONDITION$N, $VARIABLE, BLANKS}
import io.qross.pql.Solver._

import scala.collection.mutable.ListBuffer
import io.qross.fs.Path._

class Condition(val expression: String) {

    private val $OPERATOR = """(?i)===|!==|==|!=|<>|>=|<=|>|<|=|\sNOT\s+IN\b|\sIS\s+NOT\s|\sIN\b|\sIS\s|\sAND\s|\sOR\s|\bNOT\s+EXISTS\b|\bEXISTS\b|^NOT\s""".r

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

    val (field: String, value: String, operator: String) = {
        $OPERATOR.findFirstIn(expression) match {
            case Some(opt) =>
                (expression.takeBefore(opt).trim(),
                    expression.takeAfter(opt).trim(),
                    opt.trim.toUpperCase.replaceAll(BLANKS, "\\$"))
            case None => ("", expression, "")
        }
    }

    private def evalField(PQL: PQL, group: ConditionGroup): DataCell = {
        if (field == "") {
            DataCell.NULL
        }
        else if ($CONDITION$N.test(field)) {
            DataCell(group.conditions(field.$trim("~condition[", "]").toInt).eval(PQL, group), DataType.BOOLEAN)
        }
        else if ($VARIABLE.test(field)) {
            PQL.findVariable(field)
        }
        else if (value.equalsIgnoreCase("UNDEFINED") || value.equalsIgnoreCase("DEFINED")) {
            field.popStash(PQL).toDataCell(DataType.TEXT)
        }
        else {
            field.$sharp(PQL)
        }
    }

    private def evalValue(PQL: PQL, group: ConditionGroup): DataCell = {
        if ($CONDITION$N.test(value)) {
            DataCell(group.conditions(value.$trim("~condition[", "]").toInt).eval(PQL, group), DataType.BOOLEAN)
        }
        else if ($VARIABLE.test(value)) {
            PQL.findVariable(value)
        }
        else if (value.equalsIgnoreCase("EMPTY")) {
            DataCell.EMPTY
        }
        else if (value.equalsIgnoreCase("UNDEFINED")) {
            DataCell.UNDEFINED
        }
        else if (value.equalsIgnoreCase("NULL") || value == "()") {
            DataCell.NULL
        }
        else if (operator == "NOT") {
            value.replace("==", "DOUBLE$EQUALITY$SIGN")
              .replace("!=", "INEQUALITY$SIGN")
              .replace("=", "==")
              .replace("DOUBLE$EQUALITY$SIGN", "==")
              .replace("INEQUALITY$SIGN", "!=")
              .replace("<>", "!=").$sharp(PQL, "\"")
        }
        else {
            value.$sharp(PQL, "\"")
        }
    }

    def eval(PQL: PQL, group: ConditionGroup): Boolean = {
        if (Set[String]("AND", "OR", "NOT", "EXISTS", "NOT$EXISTS").contains(operator)) {
            operator match {
                case "AND" =>
                    val left = evalField(PQL, group).asBoolean(false)
                    if (!left) {
                        false
                    }
                    else {
                        evalValue(PQL, group).asBoolean(false)
                    }
                case "OR" =>
                    val left = evalField(PQL, group).asBoolean(false)
                    if (left) {
                        true
                    }
                    else {
                        evalValue(PQL, group).asBoolean(false)
                    }
                case "NOT" =>
                    !evalValue(PQL, group).asBoolean(false)
                case "EXISTS" =>
                    if (field == "") {
                        value.$trim("(", ")").$trim("[", "]").trim() != ""
                    }
                    else {
                        field.toUpperCase() match {
                            case "FILE" | "DIR" => value.fileExists
                            case _ => false
                        }
                    }
                case "NOT$EXISTS" =>
                    if (field == "") {
                        value.$trim("(", ")").$trim("[", "]").trim() == ""
                    }
                    else {
                        field.toUpperCase() match {
                            case "FILE" | "DIR" => !value.fileExists
                            case _ => true
                        }
                    }
                case _ => false
            }
        }
        else {
            val left = evalField(PQL, group)
            val right = evalValue(PQL, group)
            this.operator match {
                case "IN" =>
                    if (right.isText) {
                        val chars = new ListBuffer[String]
                        right.asText
                            .pickChars(chars)
                            .$trim("(", ")")
                            .split(",")
                            .toSet[String]
                            .map(_.restoreChars(chars).removeQuotes())
                            .contains(left.asText.removeQuotes())
                    }
                    else {
                        right.asList.toSet.contains(left.value)
                    }
                case "NOT$IN" =>
                    if (right.isText) {
                        val chars = new ListBuffer[String]
                        !right.asText
                            .pickChars(chars)
                            .$trim("(", ")")
                            .split(",")
                            .toSet[String]
                            .map(_.restoreChars(chars).removeQuotes())
                            .contains(left.asText.removeQuotes())
                    }
                    else {
                        !right.asList.toSet.contains(left.value)
                    }
                case "IS$NOT" =>
                    val v = right.asText
                    if (v == null) {
                        left.valid && left.value != null
                    }
                    else if ("""(?i)^[a-z]+$""".r.test(v)) {
                        v.toUpperCase match {
                            case "NULL" => left.valid && left.value != null
                            case "EMPTY" => left.nonEmpty
                            case "UNDEFINED" =>
                                if (left.undefined || left.value == "UNDEFINED") {
                                    false
                                }
                                else if (left.asText.removeQuotes().containsArguments) {
                                    false
                                }
                                else {
                                    true
                                }
                            case "TABLE" | "DATATABLE" => !left.isTable
                            case "ROW" | "DATAROW" => !left.isRow
                            case "LIST" | "ARRAY" => !left.isJavaList
                            case "STRING" | "TEXT" => !left.isText
                            case "DECIMAL" | "NUMERIC" => !left.isDecimal
                            case "INT" | "INTEGER" => !left.isInteger
                            case "BOOL" | "BOOLEAN" => !left.isBoolean
                            case "DATETIME" => !left.isDateTime
                            case _ => left.dataType.typeName != v
                        }
                    }
                    else {
                        left.value != right.value
                    }
                case "IS" =>
                    val v = right.asText
                    if (v == null) {
                        left.invalid || left.value == null
                    }
                    else if ("""(?i)^[a-z]+$""".r.test(v)) {
                        v.toUpperCase match {
                            case "NULL" => left.invalid || left.value == null
                            case "EMPTY" => left.isEmpty
                            case "DEFINED" =>
                                if (left.undefined || left.value == "UNDEFINED") {
                                    false
                                }
                                else if (left.asText.removeQuotes().containsArguments) {
                                    false
                                }
                                else {
                                    true
                                }
                            case "UNDEFINED" =>
                                if (left.undefined || left.value == "UNDEFINED") {
                                    true
                                }
                                else if (left.asText.removeQuotes().containsArguments) {
                                    true
                                }
                                else {
                                    false
                                }
                            case "TABLE" | "DATATABLE" => left.isTable
                            case "ROW" | "DATAROW" => left.isRow
                            case "LIST" | "ARRAY" => left.isJavaList
                            case "STRING" | "TEXT" => left.isText
                            case "DECIMAL" | "NUMERIC" => left.isDecimal
                            case "INT" | "INTEGER" => left.isInteger
                            case "BOOL" | "BOOLEAN" => left.isBoolean
                            case "DATETIME" => left.isDateTime
                            case _ => left.dataType.typeName == v
                        }
                    }
                    else {
                        left.value == right.value
                    }
                case "===" => left.dataType == right.dataType && left.value == right.value
                case "!==" => left.dataType != right.dataType || left.value != right.value
                case "=" | "==" =>
                    // null == undefined
                    {
                        if (left.undefined || left.value == null) {
                            "null"
                        }
                        else {
                            left.asText
                        }
                    }.equalsIgnoreCase({
                        if (right.undefined || right.value == null) {
                            "null"
                        }
                        else {
                            right.asText
                        }
                    })
                case "!=" | "<>" =>
                    ! {
                        {
                            if (left.undefined || left.value == null) {
                                "null"
                            }
                            else {
                                left.asText
                            }
                        }.equalsIgnoreCase({
                            if (right.undefined || right.value == null) {
                                "null"
                            }
                            else {
                                right.asText
                            }
                        })
                    }
                case ">=" => left >= right
                case "<=" => left <= right
                case ">" => left > right
                case "<" => left < right
                case _ => right.asBoolean(false)
            }
        }
    }
}
