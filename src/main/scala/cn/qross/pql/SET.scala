package cn.qross.pql

import cn.qross.core.{DataCell, DataType}
import cn.qross.exception.SQLParseException
import cn.qross.ext.Output
import cn.qross.ext.TypeExt._
import cn.qross.pql.Patterns._
import cn.qross.pql.Solver._

import scala.collection.mutable

object SET {
    def parse(sentence: String, PQL: PQL): Unit = {
        $SET.findFirstMatchIn(sentence) match {
            case Some(m) =>
                PQL.PARSING.head.addStatement(
                    new Statement("SET", sentence, new SET(m.group(1).trim(), m.group(2).trim(), m.group(3).trim()))
                )
            case None => throw new SQLParseException("Incorrect SET sentence: " + sentence)
        }
    }
}

class SET(val declare: String, val symbol: String, val expression: String) {

    val variables: mutable.ArrayBuffer[String] = mutable.ArrayBuffer[String](declare.split(",").map(_.trim().toLowerCase()): _*)

    def execute(PQL: PQL): Unit = {
        //1. SELECT查询  - 以SELECT开头 - 需要解析$开头的变量和函数
        //2. 非SELECT查询 - 以INSERT,UPDATE,DELETE开头 - 需要解析$开头的变量和函数
        //3. 字符串赋值或连接 - 用双引号包含，变量和内部函数需要加$前缀 - 需要解析$开头的变量和函数
        //4. 执行函数 - 以函数名开头，不需要加$前缀 - 直接执行函数
        //5. 变量间直接赋值 - 是变量格式(有$前缀)且是存在于变量列表中的变量 - 直接赋值
        //6. 数学表达式 - 其他 - 解析函数和变量然后求值，出错则抛出异常

        val result = {
            if (expression != "") {
                expression.$compute(PQL)
            } else {
                DataCell.UNDEFINED
            }
        }

//        if (variables.length == 1 && PQL.containsVariable(variables(0))) {
//            PQL.updateVariable(variables.head, result)
//        }
        if (symbol == ":=") {
            if (result.isTable) {
                result.asTable.firstRow match {
                    case Some(row) =>
                        for (i <- variables.indices) {
                            if (i < row.size) {
                                PQL.updateVariable(variables(i), row.getCell(i))
                            }
                            else {
                                PQL.updateVariable(variables(i), DataCell.NULL)
                            }
                        }
                    //throw new SQLExecuteException("Columns amount in SELECT must equals variables number.")
                    case None =>
                        for (i <- variables.indices) {
                            PQL.updateVariable(variables(i), DataCell.NULL)
                        }
                }
            }
            else if (result.isJavaList) {
                val list = result.asJavaList
                for (i <- variables.indices) {
                    if (i < list.size()) {
                        PQL.updateVariable(variables(i), DataCell(list.get(i)))
                    }
                    else {
                        PQL.updateVariable(variables(i), DataCell.NULL)
                    }
                }
            }
            else if (result.isRow) {
                val row = result.asRow
                for (i <- variables.indices) {
                    if (i < row.size) {
                        PQL.updateVariable(variables(i), row.getCell(i))
                    }
                    else {
                        PQL.updateVariable(variables(i), DataCell.NULL)
                    }
                }
            }
            else {
                //单值
                PQL.updateVariable(variables.head, result)
                if (variables.length > 1) {
                    for (i <- 1 until variables.length) {
                        PQL.updateVariable(variables(i), DataCell.NULL)
                    }
                }
            }
        }
        else if (symbol == "=:") {
            PQL.updateVariable(variables.head, result)
        }
    }
}
