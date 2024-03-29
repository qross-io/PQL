package cn.qross.fql

import cn.qross.core.{DataRow, DataTable, DataType}
import cn.qross.exception.{SQLExecuteException, SQLParseException}
import cn.qross.ext.TypeExt._

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

//FQL = fragment query language

object Fragment {
    val $VALUES: Regex = """(?i)\sVALUES\s""".r
    val $MULTI$VALUES: String = """\)\s*,\s*\("""

}

class Fragment(val phrase: String) {

    def insertInto(table: DataTable): DataTable = {
        if (Fragment.$VALUES.test(phrase)) {
            val fields = phrase.takeBeforeX(Fragment.$VALUES).$trim("(", ")").split(",").map(_.trim())
            phrase.takeAfterX(Fragment.$VALUES).$trim("(", ")")
                    .split(Fragment.$MULTI$VALUES)
                    .map(vs => {
                        val list = new ListBuffer[String]()
                        var pre = ' '
                        var pri = 0
                        for (i <- vs.indices) {
                            val c = vs.charAt(i)
                            if (c == ',') {
                                if (pre != '\'' && pre != '"') {
                                    list += vs.substring(pri, i).trim()
                                    pri = i + 1
                                }
                            }
                            else if (c == '\'') {
                                if (pre == ' ') {
                                    pre = '\''
                                }
                                else if (pre == '\'' && vs.charAt(i - 1) != '\\') {
                                    pre = ' '
                                }
                            }
                            else if (c == '"') {
                                if (pre == ' ') {
                                    pre = '"'
                                }
                                else if (pre == '"' && vs.charAt(i - 1) != '\\') {
                                    pre = ' '
                                }
                            }
                        }
                        list += vs.substring(pri).trim()
                        list
                    })
                .foreach(values => {
                    if (fields.length == values.length) {
                        val row = new DataRow()
                        for (i <- fields.indices) {
                            if (table.contains(fields(i))) {
                                val dataType = table.getFieldDataType(fields(i))
                                row.set(fields(i), {
                                    dataType match {
                                        case DataType.TEXT => values(i).removeQuotes()
                                        case DataType.INTEGER => values(i).toLong
                                        case DataType.DECIMAL => values(i).toDouble
                                        case DataType.DATETIME => values(i).toDateTime
                                        case DataType.BOOLEAN => values(i).toBoolean(false)
                                        case _ => values(i).removeQuotes()
                                    }
                                }, dataType)
                            }
                            else {
                                if (values(i).quotesWith("'") || values(i).quotesWith("\"")) {
                                    row.set(fields(i), values(i).removeQuotes(), DataType.TEXT)
                                }
                                else if ("""^-?\d+$""".r.test(values(i))) {
                                    row.set(fields(i), values(i), DataType.INTEGER)
                                }
                                else if ("""^-?\d+\.\d+$""".r.test(values(i))) {
                                    row.set(fields(i), values(i).toDouble, DataType.DECIMAL)
                                }
                                else {
                                    row.set(fields(i), values(i), DataType.TEXT)
                                }
                            }
                        }
                        table.addRow(row)
                    }
                    else {
                        throw new SQLExecuteException("Column count doesn't match value count. " + values.mkString(", "))
                    }
                })
        }
        else {
            throw new SQLParseException("Incorrect fragment format. The correct format is: (column1, column2, ...) VALUES (value1, value2, ...), ..." )
        }

        table
    }

    //提取字符串
    //提取并计算函数
    //提取并计算嵌套小括号
    //提取并计算基本表达式  + - * / %
    //CASE WHEN  END
    // A = B

    //"WHERE a % 2=1 AND LEFT(b, 2)='2' AND (true || false)"
    def where(table: DataTable): DataTable = {
        val group = new ConditionGroup(phrase).parse()
        val other = new DataTable()
        table.foreach(row => {
            if (group.where(row)) {
                other.addRow(row)
            }
        })

        if (other.isEmpty) {
            table.getColumns.foreach(col => {
                other.addField(col._1, col._2)
            })
        }

        //保持数据结构
        other
    }

    def delete(table: DataTable): DataTable = {
        val group = new ConditionGroup(phrase).parse()
        val other = new DataTable()
        table.foreach(row => {
            if (!group.where(row)) {
                other.addRow(row)
            }
        })

        //保持数据结构
        if (other.isEmpty) {
            table.getColumns.foreach(col => {
                other.addField(col._1, col._2)
            })
        }

        other
    }

    def update(table: DataTable): DataTable = {
        //SET a = 1, b = 2
        val set = new SET(phrase)
        table.foreach(row => {
            set.pieces.foreach(s => {
                row.set(s.key, s.compute(row))
            })
        })
        table
    }

    def call(function: String): String = {
        ""
    }
}
