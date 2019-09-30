package io.qross.pql

import io.qross.core.DataType.DataType
import io.qross.core.{DataCell, DataType}
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.net.Json._
import io.qross.pql.Patterns._
import io.qross.pql.Solver._

class SET(var declare: String, expression: String) {

    def assign(PQL: PQL): Unit = {
        //1. SELECT查询  - 以SELECT开头 - 需要解析$开头的变量和函数
        //2. 非SELECT查询 - 以INSERT,UPDATE,DELETE开头 - 需要解析$开头的变量和函数
        //3. 字符串赋值或连接 - 用双引号包含，变量和内部函数需要加$前缀 - 需要解析$开头的变量和函数
        //4. 执行函数 - 以函数名开头，不需要加$前缀 - 直接执行函数
        //5. 变量间直接赋值 - 是变量格式(有$前缀)且是存在于变量列表中的变量 - 直接赋值
        //6. 数学表达式 - 其他 - 解析函数和变量然后求值，出错则抛出异常

        val variables: Array[(DataType, String)] = {
            declare.split(",")
                .map(_.trim)
                .map(v => {
                    if ($BLANK.test(v)) {
                        val dataType = v.takeAfter($BLANK).trim()
                        val name = v.takeBefore($BLANK).trim()
                        if ($DATA_TYPE.test(dataType)) {
                            (DataType.ofName(dataType), name)
                        }
                        else {
                            throw new SQLParseException("Wrong data type: " + dataType)
                        }
                    }
                    else {
                        (DataType.AUTO, v)
                    }
                })
        }

        if ($SELECT.test(expression)) { //SELECT

            val result = new SELECT(expression).execute(PQL)

            if (variables.length > 1) {
                result.asTable.firstRow match {
                    case Some(row) =>
                        if (row.size >= variables.length) {
                            for (i <- variables.indices) {
                                PQL.updateVariable(variables(i)._2, row.getCell(i).to(variables(i)._1))
                            }
                        }
                        else {
                            throw new SQLExecuteException("Columns amount in SELECT must equals variables number.")
                        }
                    case None =>
                        for (i <- variables.indices) {
                            PQL.updateVariable(variables(i)._2, DataCell(null, variables(i)._1))
                        }
                }

            }
            else {
                PQL.updateVariable(variables.head._2,
                    variables.head._1 match {
                            case DataType.TABLE => result.toTable
                            case DataType.ROW => result.toRow
                            case DataType.ARRAY => result.toJavaList
                            case _ =>
                                result.asTable.firstRow match {
                                    case Some(row) =>
                                        val data = row.getCell(0)
                                        if (data.notFound) {
                                            DataCell.NULL
                                        }
                                        else {
                                            data
                                        }
                                    case None => DataCell.NULL
                                }
                        }
                )
            }
        }
        else if ($PARSE.test(expression)) {
            //暂不支持SHARP表达式
            val path = expression.takeAfter($BLANK).trim.$restore(PQL)

            if (variables.nonEmpty) {
                val row = PQL.dh.parseRow(path)
                if (row.size >= variables.length) {
                    for (i <- variables.indices) {
                        PQL.updateVariable(variables(i)._2, row.getCell(i).to(variables(i)._1))
                    }
                }
                else {
                    throw new SQLExecuteException("Columns amount in PARSE must equals variables number.")
                }
            }
            else {
                PQL.updateVariable(variables.head._2,
                    variables.head._1 match {
                        case DataType.AUTO | DataType.JSON => DataCell(PQL.dh.parseNode(path), DataType.JSON)
                        case DataType.TABLE => DataCell(PQL.dh.parseTable(path), DataType.TABLE)
                        case DataType.ROW => DataCell(PQL.dh.parseRow(path), DataType.ROW)
                        case DataType.ARRAY => DataCell(PQL.dh.parseList(path), DataType.ARRAY)
                        case _ => PQL.dh.parseValue(path)
                    }
                )
            }
        }
        else if ($NON_QUERY.test(expression)) {
            //INSERT + UPDATE + DELETE
            if (variables.length == 1) {
                PQL.updateVariable(variables.head._2, DataCell(PQL.dh.executeNonQuery(expression.$restore(PQL)), variables.head._1))
            }
            else {
                throw new SQLParseException("Only 1 variable name allowed when save affected rows of an INSERT/UPDATE/DELETE sentence. " + expression)
            }
        }
        else {
            //在SHARP表达式内部再恢复字符串和中间值
            if (variables.length == 1) {
                val data = new SHARP(expression.$clean(PQL)).execute(PQL)
                PQL.updateVariable(variables.head._2, {
                    if (variables.head._1 == DataType.AUTO) {
                        data
                    }
                    else {
                        data.to(variables.head._1)
                    }
                })
            }
            else {
                throw new SQLParseException("Only 1 variable name allowed when declare a new variable. " + expression)
            }
        }
    }
}
