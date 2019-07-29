package io.qross.sql

import io.qross.ext.TypeExt._
import io.qross.sql.Patterns.$BLANK

object OUTPUT {
    val TABLE: String = "TABLE" //表
    val ROW: String = "ROW" //数据行
    val VALUE: String = "VALUE" //单值
    val ARRAY: String = "ARRAY" //数组或列表
    val LIST: String = "LIST" //列表, 同ARRAY
    val MAP: String = "MAP" //同ROW
    val OBJECT: String = "OBJECT" //同ROW
    val AFFECTED: String = "AFFECTED" //影响的行数
    val JSON: String = "JSON" //自定义Json格式
}

class OUTPUT(var outputType: String, var content: String) {
    val caption: String = content.takeBefore($BLANK).toUpperCase()
    if (outputType == null || outputType.trim() == "") {
        outputType = if (caption == "SELECT" || caption == "PARSE") {
                         OUTPUT.TABLE
                     }
                     else if (caption == "LET") {
                         OUTPUT.VALUE
                     }
                     else if (Patterns.NON_QUERY_CAPTIONS.contains(caption)) {
                         OUTPUT.AFFECTED
                     }
                     else {
                         OUTPUT.JSON
                     }
    }
    else {
        outputType = outputType.trim().toUpperCase()
    }
    if (caption == "PARSE") {
        content = content.takeAfter($BLANK).trim()
    }
}