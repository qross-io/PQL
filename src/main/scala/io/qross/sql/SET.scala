package io.qross.sql

import io.qross.core.{DataCell, DataHub, DataType}
import io.qross.ext.TypeExt._
import io.qross.sql.Patterns._
import io.qross.sql.Solver._
import io.qross.core.DataType.DataType
import io.qross.ext.Output
import io.qross.net.Json._

class SET(var variable: String, expression: String) {

    val dataType: DataType = if (variable.contains(",")) {
                                DataType.AUTO
                            }
                            else {
                                if ($BLANK.test(variable)) {
                                    if ($DATATYPE.test(variable)) {
                                        val dt = variable.takeBefore($BLANK)
                                        variable = variable.takeAfter($BLANK).trim()
                                        DataType.ofName(dt)
                                    }
                                    else {
                                        Output.writeWarning(s"Wrong data type : " + variable)
                                        DataType.AUTO
                                    }
                                }
                                else {
                                    DataType.AUTO
                                }
                            }

    val variables: Array[(DataType, String)] = if (variable.contains(",")) {
                                                    if (!$SELECT.test(expression) && !$PARSE.test(expression)) {
                                                        throw new SQLParseException("Multiple variables definition only support SELECT or PARSE sentence. " + expression)
                                                    }
                                                    variable.split(",")
                                                            .map(_.trim)
                                                            .map(v => {
                                                                if ($BLANK.test(v)) {
                                                                    if ($DATATYPE.test(v)) {
                                                                        (DataType.ofName(v.takeBefore($BLANK)), v.takeAfter($BLANK).trim())
                                                                    }
                                                                    else {
                                                                        Output.writeWarning(s"Wrong data type : " + v)
                                                                        (DataType.AUTO, v)
                                                                    }
                                                                }
                                                                else {
                                                                    (DataType.AUTO, v)
                                                                }
                                                            })
                                                }
                                                else {
                                                    new Array[(DataType, String)](0)
                                                }

    def assign(PSQL: PSQL): Unit = {
        //1. SELECT查询  - 以SELECT开头 - 需要解析$开头的变量和函数
        //2. 非SELECT查询 - 以INSERT,UPDATE,DELETE开头 - 需要解析$开头的变量和函数
        //3. 字符串赋值或连接 - 用双引号包含，变量和内部函数需要加$前缀 - 需要解析$开头的变量和函数
        //4. 执行函数 - 以函数名开头，不需要加$前缀 - 直接执行函数
        //5. 变量间直接赋值 - 是变量格式(有$前缀)且是存在于变量列表中的变量 - 直接赋值
        //6. 数学表达式 - 其他 - 解析函数和变量然后求值，出错则抛出异常
        var exp = this.expression
        if ($SELECT.test(exp)) { //SELECT
            exp = exp.$restore(PSQL)

            if (variables.nonEmpty) {
                val row = PSQL.dh.executeDataRow(exp)
                if (row.size >= variables.length) {
                    for (i <- variables.indices) {
                        PSQL.updateVariable(variables(i)._2, row.getCell(i).to(variables(i)._1))
                    }
                }
                else {
                    throw new SQLExecuteException("Columns amount in SELECT must equals variables number.")
                }
            }
            else {
                PSQL.updateVariable(variable,
                        dataType match {
                            case DataType.TABLE => DataCell(PSQL.dh.executeDataTable(exp), DataType.TABLE)
                            case DataType.ROW => DataCell(PSQL.dh.executeDataRow(exp), DataType.ROW)
                            case DataType.ARRAY => DataCell(PSQL.dh.executeSingleList(exp), DataType.ARRAY)
                            case _ => PSQL.dh.executeSingleValue(exp)
                        }
                )
            }
        }
        else if ($PARSE.test(exp)) {
            exp = exp.takeAfter($BLANK).trim.$restore(PSQL)

            if (variables.nonEmpty) {
                val row = PSQL.dh.parseRow(exp)
                if (row.size >= variables.length) {
                    for (i <- variables.indices) {
                        PSQL.updateVariable(variables(i)._2, row.getCell(i).to(variables(i)._1))
                    }
                }
                else {
                    throw new SQLExecuteException("Columns amount in PARSE must equals variables number.")
                }
            }
            else {
                PSQL.updateVariable(variable,
                    dataType match {
                        case DataType.AUTO | DataType.JSON => DataCell(PSQL.dh.parseNode(exp), DataType.JSON)
                        case DataType.TABLE => DataCell(PSQL.dh.parseTable(exp), DataType.TABLE)
                        case DataType.ROW => DataCell(PSQL.dh.parseRow(exp), DataType.ROW)
                        case DataType.ARRAY => DataCell(PSQL.dh.parseList(exp), DataType.ARRAY)
                        case _ => PSQL.dh.parseValue(exp)
                    }
                )
            }
        }
        else if ($NON_QUERY.test(exp)) {
            //INSERT + UPDATE + DELETE
            exp = exp.$restore(PSQL)
            PSQL.updateVariable(variable, DataCell(PSQL.dh.executeNonQuery(exp), DataType.INTEGER))
        }
        else {
            exp = exp.$clean(PSQL) //在SHARP表达式内部再恢复字符串和中间值
            PSQL.updateVariable(variable, new SHARP(exp).execute(PSQL))
        }
    }
}
