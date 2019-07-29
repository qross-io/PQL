package io.qross.core

import io.qross.ext.TypeExt._

import scala.util.matching.Regex
import scala.util.matching.Regex.Match

object Parameter {
    val PARAMETER: Regex = """(^|[^#&])((#|&)\(?([a-zA-Z0-9_]+)\)?)""".r //DataHub传递参数, #name 或 #(name) 或 &name 或 &(name)

    implicit class Sentence(var sentence: String) {
        def hasParameters: Boolean = {
            PARAMETER.test(sentence)
        }

        def matchParameters: List[Match] = {
            PARAMETER.findAllMatchIn(sentence).toList
        }

        //适用于DataHub pass和put的方法, 对应DataSource的 tableSelect和tableUpdate
        def replaceParameters(row: DataRow): String = {

            PARAMETER
                    .findAllMatchIn(sentence)
                    .toList
                    .sortBy(m => m.group(4))
                    .reverse
                    .foreach(m => {

                        val whole = m.group(0)
                        val fieldName = m.group(4)
                        val symbol = m.group(3)
                        val prefix = m.group(1) //前缀
                        val suffix = if (!m.group(2).contains("(") && m.group(2).contains(")")) ")" else ""

                        if (symbol == "#") {
                            if (row.contains(fieldName)) {
                                sentence = sentence.replace(whole, prefix + row.getString(fieldName) + suffix)
                            }
                        }
                        else if (symbol == "&") {
                            if (row.contains(fieldName)) {
                                sentence = sentence.replace(whole, (row.getDataType(fieldName), row.get(fieldName)) match {
                                    case (Some(dataType), Some(value)) =>
                                        if (value == null) {
                                            prefix + "NULL" + suffix
                                        }
                                        else if (dataType == DataType.INTEGER || dataType == DataType.DECIMAL) {
                                            prefix + value.toString + suffix
                                        }
                                        else {
                                            prefix + "'" + value.toString.replace("'", "''") + "'" + suffix
                                        }
                                    case _ => prefix
                                })
                            }
                        }
                    })

            sentence = sentence.replace("~u0023", "#").replace("~u0026", "&")

            sentence
        }
    }
}
