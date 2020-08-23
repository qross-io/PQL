package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.exception.SQLParseException
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.net.Json._
import io.qross.pql.Patterns.{$PARSE, ARROW}
import io.qross.pql.Solver._

object PARSE {
    //用于PQL表达式解析
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("PARSE", sentence, new PARSE(sentence)))
    }
}

class PARSE(val sentence: String) {

    //sentence包含PARSE关键词

    //express 是否支持嵌入式查询语句, 即 ${{ }}
    //不能用parse名, 会与静态方法
    def doParse(PQL: PQL, express: Int = Solver.FULL): DataCell = {
        sentence.$process(PQL, express, body => {
            val plan = Syntax("PARSE").plan(body.drop(5).trim())
            val path = plan.headArgs
            plan.head match {
                case "" =>
                    if (plan.size > 1) {
                        plan.last match {
                            case "AS TABLE" => PQL.dh.parseTable(path).toDataCell(DataType.TABLE)
                            case "AS ROW" => PQL.dh.parseRow(path).toDataCell(DataType.ROW)
                            case "AS LIST" | "AS ARRAY" => PQL.dh.parseList(path).toDataCell(DataType.ARRAY)
                            case "AS VALUE" => PQL.dh.parseValue(path)
                            case _ => PQL.dh.parseTable(path).toDataCell(DataType.TABLE)
                        }
                    }
                    else {
                        PQL.dh.parseTable(path).toDataCell(DataType.TABLE)
                    }
                case _ => PQL.dh.parseTable(path).toDataCell(DataType.TABLE)
            }
        })
    }

    def execute(PQL: PQL): Unit = {
        val data = this.doParse(PQL)

        PQL.WORKING += data.value
        PQL.COUNT_OF_LAST_SELECT = if (data.isTable) data.asTable.size else if (data.isJavaList) data.asJavaList.size() else 1

        if (PQL.dh.debugging) {
            if (data.isTable) {
                data.asTable.show()
            }
            else if (data.isRow) {
                println(data.asRow.toString)
            }
            else if(data.isJavaList) {
                Json.serialize(data.asJavaList).print
            }
            else {
                println(data.asText)
            }
        }
    }
}