package cn.qross.fql

import cn.qross.core.{DataCell, DataRow, DataTable, DataType}
import cn.qross.exception.SQLParseException
import cn.qross.ext.TypeExt._

import scala.collection.mutable
import scala.util.control.Breaks._
import cn.qross.fql.Patterns._
import cn.qross.script.Script


object SELECT {


}

class SELECT(statement: String, processing: Boolean) {

    def this(statement: String) {
        this(statement, true)
    }

    //val tables = ???  //表名和别名

    //val INs = ???  //where中的in  不支持联合查询
    //val JOIN = ""  //INNER/OUTER  ON

    private[qross] val chars = new mutable.ListBuffer[String]()
    private[qross] val functions = new mutable.ListBuffer[String]()
    private[qross] val queries = new mutable.ListBuffer[String]()

    private[qross] val fields = new mutable.ArrayBuffer[(String, String)]() //转换前的字段
    private[qross] val columns = new mutable.ArrayBuffer[Column]() //识别后的字段列表

    private[qross] val conditions = new mutable.ArrayBuffer[Condition]() //where
    private val booleans = new mutable.ArrayBuffer[Boolean]()

    private var phrase: String = "" //下一个关键词
    private var sentence: String = statement.trim()

    //processing 表示第一次处理字符串、函数和子查询, 在子查询中不做处理
    if (processing) {
        //处理字符串
        sentence = sentence.pickChars(chars)

        //处理函数
        // IN (SELECT) - AND/OR ( 都有问题
        //    breakable {
        //        while (true) {
        //            $FUNCTION.findFirstIn(sentence) match {
        //                case Some(function) =>
        //                    sentence = sentence.replace(function, "~function[" + functions.size + "]")
        //                    functions += function
        //                case None => break
        //            }
        //        }

        //处理子查询 - 子查询只能在 IN 和 FROM 里
        //    breakable {
        //        while (true) {
        //            $QUERY.findFirstIn(sentence) match {
        //                case Some(query) =>
        //                    sentence = sentence.replace(query, "~select[" + queries.size + "]")
        //                    queries += query
        //                case None => break
        //            }
        //        }
        //    }
    }

    $SELECT.findFirstMatchIn(sentence) match {
        case Some(m) =>
            sentence = sentence.substring(m.group(0).length - m.group(2).length).trim()
            m.group(1).trim()
                .split(",")
                .map(field => {
                    if (field.contains(" AS ")) {
                        fields += ((field.takeBefore(" AS ").trim(), field.takeAfter(" AS ").trim()))
                    }
                    else {
                        fields += ((field.trim(), field.trim()))
                    }
                })
        case None => throw new SQLParseException("Wrong SELECT sentence: " + sentence)
    }

    val (from: String, seek: Long) = parseFrom(sentence)

    if (phrase == "WHERE") {
        $WHERE.findFirstMatchIn(sentence) match {
            case Some(m) =>
                sentence = sentence.substring(m.group(0).length - m.group(2).length).trim()
                phrase = m.group(2).toUpperCase()
                this.conditions ++= new ConditionGroup(m.group(1).trim()).parse().conditions
            case None =>
        }
    }

    //支持1, 2, 3 按数字位置获取字段
    val GROUP$BY: String = null

    val HAVING: String = null

//    if (phrase == "ORDER") {
//        $ORDER$BY.findFirstMatchIn(sentence) match {
//            case Some(m) =>
//                sentence = sentence.substring(m.group(0).length - m.group(2).length).trim()
//                phrase = m.group(2).toUpperCase()
//                new ORDER$BY(m.group(1).trim())
//            case None => throw new SQLParseException("Wrong ORDER BY phrase: " + sentence)
//        }
//    }
//    else {
//        null
//    }

    //limit 起始行, 从0开始
    //limit 限制行, 达到limit时停止
    val (start: Int, limit: Int) = {
        if (phrase == "LIMIT") {
            $LIMIT.findFirstMatchIn(sentence) match {
                case Some(m) =>
                    val exp = m.group(1).trim()
                    if (exp.contains(",")) {
                        (
                            exp.takeBefore(",").trim().toInteger(0).toInt,
                            exp.takeAfter(",").trim().toInteger(-1).toInt
                        )
                    }
                    else {
                        (0, exp.toInteger(-1).toInt)
                    }
                case None => (0, -1)
            }
        }
        else {
            (0, -1)
        }
    }
    val most: Int = start + limit

    def turnColumns(meta: mutable.LinkedHashMap[String, DataType]): Unit = {
        fields.foreach(col => {
            if (col._1.bracketsWith("~str[", "]")) {
                columns += new Column(col._2, col._1, Column.CONSTANT, DataType.TEXT, col._1.restoreChars(chars).removeQuotes())
            }
            else if ("""^-?\d+$""".r.test(col._1)) {
                columns += new Column(col._2, col._1, Column.CONSTANT, DataType.INTEGER, col._1.toInteger)
            }
            else if ("""^-?\d+\.\d+$""".r.test(col._1)) {
                columns += new Column(col._2, col._1, Column.CONSTANT, DataType.DECIMAL, col._1.toDecimal)
            }
            else if ("(?i)^true|false$".r.test(col._1)) {
                columns += new Column(col._2, col._1, Column.CONSTANT, DataType.BOOLEAN, col._1.toBoolean(false))
            }
            else if ("""(?)^null$""".r.test(col._1)) {
                columns += new Column(col._2, col._1, Column.CONSTANT, DataType.TEXT, null)
            }
            else if (meta.contains(col._1)) {
                columns += new Column(col._2, col._1, Column.MAP, meta(col._1), null)
            }
            else {
                columns += new Column(col._2, col._1, Column.MAP, DataType.TEXT, null)
            }
        })
    }

    def where(row: DataRow): Boolean = {
        if (conditions.nonEmpty) {
            booleans.clear()

            //最终执行
            for (condition <- conditions) {

                val field = condition.field.restoreChars(chars)
                val value = condition.value.restoreChars(chars)

                booleans +=
                    Condition.eval(
                        if (field == null) {
                            DataCell.NULL
                        }
                        else if ($CONDITION$N.test(field)) {
                            booleans(field.$trim("~condition[", "]").toInt)
                        }
                        else if (row.contains(field)) {
                            row.getCell(field).value
                        }
                        else {
                            Script.evalJavascript(field)
                        },
                        condition.operator,
                        if ($CONDITION$N.test(value)) {
                            booleans(value.$trim("~condition[", "]").toInt)
                        }
                        else if (value.equalsIgnoreCase("NULL") || value == "()") {
                            null
                        }
                        else if (row.contains(value)) {
                            row.getCell(value).value
                        }
                        else {
                            Script.evalJavascript(value)
                        })
            }

            booleans.last
        }
        else {
            true
        }
    }

    private def parseFrom(expression: String): (String, Long) = {
        var from = ""
        var seek = 0L

        $FROM.findFirstMatchIn(expression) match {
            case Some(m) =>
                sentence = sentence.substring(m.group(0).length - m.group(2).length).trim()
                phrase = m.group(2).toUpperCase()
                from = m.group(1).trim()
                $SEEK.findFirstMatchIn(from) match {
                    case Some(n) =>
                        from = from.takeBefore(n.group(0)).trim()
                        seek = n.group(1).trim().toInteger(0)
                    case None =>
                }
            case None =>
        }

        (from , seek)
    }


}

class ORDER$BY(phrase: String) {
    println(phrase)
}


//执行计划
//IN (SELECT...)
//FROM (SELECT...) A
//JOIN (SELECT...) A
//EXISTS (SELECT...)  不支持

//FROM支持的类型
//FROM `mysql.qross`:database.table
//FROM $table
//FROM @buffer
//FROM :virtual