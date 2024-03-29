package cn.qross.core

import cn.qross.ext.TypeExt._

import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.matching.Regex.Match

object Parameter {
    //DataHub传递参数, #name 或 #(name) 或 &name 或 &(name)
    val $PARAMETER: List[Regex] = List[Regex](
        """(#)\(([^()]+)\)""".r,
        """(#)([a-zA-Z0-9_]+)""".r,
        """(&)\(([^()]+)\)""".r,
        """(&)([a-zA-Z0-9_]+)""".r
    )
    val $QUESTION$MARK: Regex = """\?""".r
    val $BATCH$MARK: Regex = """@\{[^{}]+\}""".r

    //符号类型
    val NONE: Int = 0
    val MARK: Int = 1
    val SHARP: Int = 2

    implicit class Sentence$Parameter(var sentence: String) {

        def hasParameters: Boolean = $PARAMETER.map(_.test(sentence)).reduce(_||_)  //这图形有点...

        def hasQuestionMark: Boolean = {
            sentence.pickChars().contains("?")
        }

        def placeHolderType: Int = {
            if (sentence.hasQuestionMark) {
                MARK
            }
            else if (sentence.hasParameters) {
                SHARP
            }
            else {
                NONE
            }
        }

        def matchBatchMark: Option[String] = {
            $BATCH$MARK.findFirstIn(sentence)
        }

        //适用于DataHub pass和put的方法, 对应DataSource的 tableSelect和tableUpdate
        def replaceParameters(row: DataRow): String = {
            sentence.replaceParameters(sentence.pickParameters(), row)
        }

        def replaceParameters(params: List[Match], row: DataRow, quote: String = "'"): String = {
            for (param <- params) {
                val whole = param.group(0)
                val field = param.group(2)
                val symbol = param.group(1)

                if (symbol == "#") {
                    if (row.contains(field)) {
                        sentence = sentence.replace(whole, row.getString(field, null).ifNull("null").preventInjection)
                    }
                }
                else if (symbol == "&") {
                    if (row.contains(field)) {
                        (row.getDataType(field), row.get(field)) match {
                            case (Some(dataType), Some(value)) =>
                                sentence = sentence.replace(whole, {
                                    if (value == null) {
                                        "null"
                                    }
                                    else if (dataType == DataType.INTEGER || dataType == DataType.DECIMAL) {
                                        value.toString
                                    }
                                    else {
                                        quote + value.toString.preventInjection + quote
                                    }
                                })
                            case _ =>
                        }
                    }
                }
            }

            sentence.replace("~u0023", "#").replace("~u0026", "&")
        }

        def replaceQuestionMarks(row: DataRow): String = {
            for (i <- 0 until row.size) {
                sentence = sentence.replace(s"~mark[$i]", row.getString(i))
            }
            sentence
        }

        def pickParameters(): List[Match] = {
            $PARAMETER.flatMap(_.findAllMatchIn(sentence)).sortBy(_.group(2)).reverse
        }

        def pickQuestionMarks(): String = {
            val chars = new mutable.ListBuffer[String]()
            sentence = sentence.pickChars(chars)
            val matches = $QUESTION$MARK.findAllIn(sentence).toArray
            for (i <- matches.indices) {
                sentence = sentence.replaceFirst(matches(i), s"~mark[$i]")
            }
            sentence.restoreChars(chars)
        }
    }
}
