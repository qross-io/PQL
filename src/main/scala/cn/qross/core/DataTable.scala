package cn.qross.core

import cn.qross.exception.{IncorrectFunctionNameException, MissingColumnAliasNameException, TableColumnNotFoundException}

import java.util
import com.fasterxml.jackson.databind.ObjectMapper
import cn.qross.ext.Output
import cn.qross.ext.TypeExt._
import cn.qross.fql.Fragment
import cn.qross.jdbc.{DataSource, JDBC}
import cn.qross.core.Parameter._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.parallel.mutable.ParArray
import scala.util.Random
import scala.util.control.Breaks._

object DataTable {
    
    def from(dataTable: DataTable): DataTable = {
        new DataTable().copy(dataTable)
    }
    
    def ofSchema(dataTable: DataTable): DataTable = {
        val table = new DataTable()
        table.columns ++= dataTable.columns
        table
    }
    
    def withFields(fields: (String, DataType)*): DataTable = {
        val table = new DataTable()
        fields.foreach(field => {
            table.addField(field._1, field._2)
        })
        table
    }
}

class DataTable() {

    def this(items: DataRow*) {
        this()
        //initial rows
        for (row <- items) {
            addRow(row)
        }
    }

    //所有field的集合, 有序索引
    private val fields = new mutable.ListBuffer[String]()
    //所有field及dataType的集合
    private val columns = new mutable.LinkedHashMap[String, DataType]()
    //所有field及label的集合，可以理解为是字段的中文名
    private val labels = new mutable.LinkedHashMap[String, String]()
    //所有行的集合
    val rows = new mutable.ArrayBuffer[DataRow]()


    //添加列
    def addField(fieldName: String, dataType: DataType): Unit = {
        addFieldWithLabel(fieldName, fieldName, dataType)
    }

    //添加列和标签
    def addFieldWithLabel(fieldName: String, labelName: String, dataType: DataType): Unit = {
        // . is illegal char in SQLite
        //val columnName = if (fieldName.contains(".")) fieldName.takeAfter(".") else fieldName
        val columnName = if (!this.contains(fieldName)) fieldName else fieldName + fields.size

        fields += columnName
        columns += columnName -> dataType
        labels += columnName -> labelName
    }

    //设置标签
    def label(alias: (String, String)*): DataTable = {
        for ((fieldName, aliaName) <- alias) {
            labels += fieldName -> aliaName
        }
        this
    }

    def label(alias: Array[String]): DataTable = {
        var i = 0
        for (key <- labels.keys.toList) {
            if (i < alias.length) {
                labels += key -> alias(i).removeQuotes()
            }
            i += 1
        }
        this
    }

    def newRow(): DataRow = {
        val row = new DataRow()
        row.fields ++= this.fields
        row.columns ++= this.columns
        row
    }

    def +=(row: DataRow): DataTable = {
        addRow(row)
    }

    //直接添加一个新行, 自动判断数据结构
    def addRow(row: DataRow): DataTable = {
        for (field <- row.getFields) {
            if (!contains(field)) {
                row.getDataType(field) match {
                    case Some(dataType) => addField(field, dataType)
                    case None =>
                }
            }
        }
        rows += row

        this
    }

    //并行处理
    def par: ParArray[DataRow] = {
        rows.par
    }

    //遍历
    def foreach(callback: DataRow => Unit): DataTable = {
        rows.foreach(row => {
            callback(row)
        })
        this
    }

    //遍历并返回新的DataTable, 原数据表内容不变
    def iterate(callback: DataRow => Unit): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            callback(row)
            table.addRow(row)
        })

        if (table.isEmpty) {
            columns.foreach(column => {
                table.addField(column._1, column._2)
            })
        }

        table
    }

    //过滤
    def filter(callback: DataRow => Boolean): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            if (callback(row)) {
                table.addRow(row)
            }
        })

        if (table.isEmpty) {
            columns.foreach(column => {
                table.addField(column._1, column._2)
            })
        }

        table
    }

    def filterNot(callback: DataRow => Boolean): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            if (!callback(row)) {
                table.addRow(row)
            }
        })

        if (table.isEmpty) {
            columns.foreach(column => {
                table.addField(column._1, column._2)
            })
        }

        table
    }

    //遍历并返回新的数据表, 结构不一样, 一行对一行
    def map(callback: DataRow => DataRow): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            table.addRow(callback(row))
        })

        table
    }

    //filter + map
    def collect(filter: DataRow => Boolean) (map: DataRow => DataRow): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            if (filter(row)) {
                table.addRow(map(row))
            }
        })

        table
    }
    
    //制表, 遍历并返回新的数据表, 一行对一表
    def table(fields: (String, DataType)*)(callback: DataRow => DataTable): DataTable = {
        val table = DataTable.withFields(fields: _*)
        rows.foreach(row => {
            table.merge(callback(row))
        })

        table
    }

    def getColumnName(fieldName: String): Option[String] = {
        if (this.columns.contains(fieldName)) {
            Some(fieldName)
        }
        else {
            var name = ""
            breakable {
                for (field <- this.fields) {
                    if (field.equalsIgnoreCase(fieldName)) {
                        name = field
                        break
                    }
                }
            }

            if (name != "") Some(name) else None
        }
    }

    //是否包含某一列
    def contains(fieldName: String): Boolean = this.getColumnName(fieldName).nonEmpty
    //行数
    def size: Int = rows.size
    //行数
    def count(): Int = rows.size
    //列数
    def columnCount: Int = columns.size
    //列数
    def width: Int = columns.size

    //聚合方法 - 待移除方法
    def count(groupBy: String*): DataTable = {
        val table = new DataTable()
        if (groupBy.isEmpty) {
            table.addRow(new DataRow("_count" -> count))
        }
        else {
            val map = new mutable.HashMap[DataRow, Int]()
            rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map.update(newRow, map(newRow) + 1)
                }
                else {
                    map.put(newRow, 1)
                }
            })
            for ((row, c) <- map) {
                row.set("_count", c)
                table.addRow(row)
            }
            map.clear()
        }
        table
    }

    //聚合方法 - 待移除方法
    def sum(fieldName: String, groupBy: String*): DataTable = {
        val table = new DataTable()
        if (groupBy.isEmpty) {
            var s = 0D
            rows.foreach(row => s += row.getDoubleOption(fieldName).getOrElse(0D))
            table.addRow(new DataRow("_sum" -> s))
        }
        else {
            val map = new mutable.HashMap[DataRow, Double]()
            rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map.update(newRow, map(newRow) + row.getDoubleOption(fieldName).getOrElse(0D))
                }
                else {
                    map.put(newRow, row.getDoubleOption(fieldName).getOrElse(0D))
                }
            })
            for ((row, s) <- map) {
                row.set("_sum", s)
                table.addRow(row)
            }
            map.clear()
        }

        table
    }

    //聚合方法 - 待移除方法
    def avg(fieldName: String, groupBy: String*): DataTable = {

        case class AVG(private val v: Double = 0D) {

            var count = 0D
            var sum = 0D
            if (v > 0) plus(v)

            def plus(v: Double): Unit = {
                count += 1
                sum += v
            }

            def get(): Double = {
                if (count == 0) {
                    0D
                }
                else {
                    sum / count
                }
            }
        }

        val table = new DataTable()
        if (groupBy.isEmpty) {
            var s = 0D
            rows.foreach(row => s += row.getDoubleOption(fieldName).getOrElse(0D))
            table.addRow(new DataRow("_avg" -> s / count))
        }
        else {
            val map = new mutable.HashMap[DataRow, AVG]()
            rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map(newRow).plus(row.getDoubleOption(fieldName).getOrElse(0D))
                }
                else {
                    map.put(newRow, AVG(row.getDoubleOption(fieldName).getOrElse(0D)))
                }
            })
            for ((row, v) <- map) {
                row.set("_avg", v.get())
                table.addRow(row)
            }
            map.clear()
        }

        table
    }

    //聚合方法 - 待移除方法
    def max(fieldName: String, groupBy: String*): DataTable = {

        case class MAX(number: Option[Double] = None) {

            var max: Option[Double] = None
            if (number.nonEmpty) compare(number)

            def compare(value: Option[Double]): Unit = {
                value match {
                    case Some(v) =>
                        max = max match {
                            case Some(a) => Some(v max a)
                            case None => Some(v)
                        }
                    case None =>
                }
            }
            def get(): Option[Double] = max
        }

        val table = new DataTable()
        if (groupBy.isEmpty) {
            val m = MAX()
            rows.foreach(row => {
                m.compare(row.getDoubleOption(fieldName))
            })
            table.addRow(new DataRow("_max" -> m.get().getOrElse("none")))
        }
        else {
            val map = new mutable.HashMap[DataRow, MAX]()
            rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map(newRow).compare(row.getDoubleOption(fieldName))
                }
                else {
                    map.put(newRow, MAX(row.getDoubleOption(fieldName)))
                }
            })
            for ((row, m) <- map) {
                row.set("_max", m.get().getOrElse("none"))
                table.addRow(row)
            }
            map.clear()
        }

        table
    }

    //聚合方法 - 待移除代码
    def min(fieldName: String, groupBy: String*): DataTable = {

        case class MIN(number: Option[Double] = None) {

            var min: Option[Double] = None
            if (number.nonEmpty) compare(number)

            def compare(value: Option[Double]): Unit = {
                value match {
                    case Some(v) =>
                        min = min match {
                            case Some(a) => Some(v min a)
                            case None => Some(v)
                        }
                    case None =>
                }
            }
            def get(): Option[Double] = min
        }

        val table = new DataTable()
        if (groupBy.isEmpty) {
            val m = MIN()
            rows.foreach(row => {
                m.compare(row.getDoubleOption(fieldName))
            })
            table.addRow(new DataRow("_min" -> m.get().getOrElse("none")))
        }
        else {
            val map = new mutable.HashMap[DataRow, MIN]()
            rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map(newRow).compare(row.getDoubleOption(fieldName))
                }
                else {
                    map.put(newRow, MIN(row.getDoubleOption(fieldName)))
                }
            })
            for ((row, m) <- map) {
                row.set("_min", m.get().getOrElse("none"))
                table.addRow(row)
            }
            map.clear()
        }

        table
    }

    //从前向后取n行
    def take(amount: Int): DataTable = {
        val table = new DataTable()
        for (i <- 0 until amount) {
            table.addRow(rows(i))
        }

        if (table.isEmpty) {
            columns.foreach(column => {
                table.addField(column._1, column._2)
            })
        }

        table
    }

    //随机取n行
    def takeSample(amount: Int): DataTable = {
        val table = new DataTable()
        Random.shuffle(rows)
            .take(if (amount < rows.length) amount else rows.length)
            .foreach(row => {
                table.addRow(row)
            })

        if (table.isEmpty) {
            columns.foreach(column => {
                table.addField(column._1, column._2)
            })
        }

        table
    }

    //数据操作

    //添加的行结构需要与table一致, 与newRow搭配, 与addRow不同, 不判断数据结构
    def insert(row: DataRow): DataTable = {
        rows += row
        this
    }

    def insertIfEmpty(row: DataRow): DataTable = {
        if (this.isEmpty) {
            insert(row)
        }
        else {
            this
        }
    }

    def insertIfNotExists(row: DataRow): DataTable = {
        var exists = false
        breakable{
            for (i <- rows.indices) {
                if (rows(i).getRow(row.fields: _*).mkString() == row.mkString()) {
                    exists = true
                    break
                }
            }
        }
        if (!exists) {
            addRow(row)
        }
        this
    }

    //按字段添加行, addRow的重载
    def insert(fields: (String, Any)*): DataTable = {
        addRow(new DataRow(fields: _*))
        this
    }

    def insertIfEmpty(fields: (String, Any)*): DataTable = {
        if (this.isEmpty) {
            insert(fields: _*)
        }
        else {
            this
        }
    }

    //按短语句添加行 (A, B) VALUES (1, '2')
    def insert(fragment: String): DataTable = {
        new Fragment(fragment).insertInto(this)
    }

    def insertIfEmpty(fragment: String): DataTable = {
        if (this.isEmpty) {
            insert(fragment)
        }
        else {
            this
        }
    }

    //按过滤器删除
    def delete(filter: DataRow => Boolean): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            if (!filter(row)) {
                table.addRow(row)
            }
        })

        if (table.isEmpty) {
            columns.foreach(column => {
                table.addField(column._1, column._2)
            })
        }

        clear()
        table
    }

    //按短语句删除 WHERE A=1 AND B='2' , 忽略WHERE关键词
    def delete(fragment: String): DataTable = {
        new Fragment(fragment).delete(this)
    }

    //delete 的反操作
    def where(fragment: String): DataTable = {
        new Fragment(fragment).where(this);
    }

    def update(setValue: DataRow => Unit): DataTable = {
        rows.foreach(row => {
            setValue(row)
        })
        this
    }

    //按过滤器修改
    def update(filter: DataRow => Boolean)(setValue: DataRow => Unit): DataTable = {
        rows.foreach(row => {
            if (filter(row)) {
                setValue(row)
            }
        })
        this
    }

    //按短语句修改 SET A=1, B='2' WHERE C=0 OR D>0
    def update(fragment: String): DataTable = {
        new Fragment(fragment).update(this)
    }

    def update(values: DataRow): DataTable = {
        rows.foreach(row => {
            values.fields.foreach(field => {
                row.set(field, values.getCell(field))
            })
        })

        this
    }

    def updateIfNull(values: DataRow): DataTable = {
        rows.foreach(row => {
            values.fields.foreach(field => {
                row.get(field) match {
                    case Some(value) =>
                        if (value == null) {
                            row.set(field, values.getCell(field))
                        }
                    case None =>
                }
            })
        })

        this
    }

    def updateIfUndefined(values: DataRow): DataTable = {
        rows.foreach(row => {
            values.fields.foreach(field => {
                if (!row.contains(field)) {
                    row.set(field, values.getCell(field))
                }
            })
        })

        this
    }

    def updateIfEmpty(values: DataRow): DataTable = {
        rows.foreach(row => {
            values.fields.foreach(field => {
                val value = row.getString(field)
                if (value == null || value == "") {
                    row.set(field, values.getCell(field))
                }
            })
        })

        this
    }

    def upsert(filter: DataRow => Boolean)(setValue: DataRow => Unit)(fields: (String, Any)*): DataTable = {
        var exists = false
        breakable {
            for(row <- rows) {
                if (filter(row)) {
                    setValue(row)
                    exists = true
                    break
                }
            }
        }
        if (!exists) {
            insert(fields: _*)
        }

        this
    }

    //仅WHERE, 同filter
    def select(filter: DataRow => Boolean): DataTable = {
        this.filter(filter)
    }

    //仅SELECT, 选择部分字段生成一个新表
    //SELECT a, b, c
    //SELECT a, b, sum(c) as sc, count() as ct
    //SELECT sum(a) as sa, max(abc) as ma
    def select(fields: String*): DataTable = {

        val table = new DataTable()

        val columns = new mutable.LinkedHashMap[String, String]() //待聚合字段，新字段名 -> 旧字段名
        val functions = new mutable.LinkedHashMap[String, (String, String)]() //聚合函数字段，新字段名 -> 函数名，排重字段列表
        lazy val counts = new mutable.HashMap[String, mutable.HashSet[String]]() //count-sets

        fields.foreach(field => {
            if (field.equals("*")) {
                this.fields.foreach(column => {
                    columns += column -> column
                })
            }
            else {
                """(?i)\sAS\s""".r.findFirstIn(field) match {
                    case Some(as) =>
                        val origin = field.takeBefore(as).trim() //旧字段名
                        val column = field.takeAfter(as).trim() //新字段名
                        if (origin.contains("(")) {
                            functions += column -> (origin.takeBefore("(").toUpperCase(), origin.takeAfter("(").dropRight(1))
                       }
                        else {
                            if (contains(origin)) {
                                columns += column -> origin
                            }
                            else {
                                throw new TableColumnNotFoundException(s"Column name '$origin' doesn't exists.")
                            }
                        }
                    case None =>
                        if (!field.contains("(")) {
                            if (contains(field)) {
                                columns += field -> field
                            }
                            else {
                                throw new TableColumnNotFoundException(s"Column name '$field' doesn't exists.")
                            }
                        }
                        else {
                            throw new MissingColumnAliasNameException(s"Please name as alias for $field use AS.")
                        }
                }
            }
        })

        if (functions.nonEmpty) {
            val map = new mutable.LinkedHashMap[String, DataRow]()
            val list = new mutable.ArrayBuffer[String]()

            //sum -
            //avg - 总数，个数
            //max - 单字段
            //min - 单字段
            //count - 单字段
            //count(a) - Set列
            //count(a+b+c)
            rows.foreach(row => {

                columns.foreach(col => {
                    list += row.getString(col._2)
                })

                val joined = list.mkString(",")
                if (!map.contains(joined)) {
                    map += joined -> new DataRow()
                    columns.foreach(column => {
                        map(joined).set(column._1, row.getCell(column._2))
                    })
                }

                list.clear()

                functions.foreach(function => {
                    val column = function._2._2 //函数内的字段名或字段名列表
                    function._2._1 match {
                        case "SUM" => //仅支持单字段
                            if (contains(column)) {
                                if (!map(joined).contains(function._1)) {
                                    map(joined).set(function._1, row.getCell(column))
                                }
                                else {
                                    map(joined).set(function._1, row.getCell(column) + map(joined).getCell(function._1))
                                }
                            }
                            else {
                                throw new TableColumnNotFoundException(s"Column name '$column' doesn't exists.")
                            }
                        case "COUNT" =>
                            if (column == "" || """^\d+$""".r.test(column)) {
                                //无字段累加
                                if (!map(joined).contains(function._1)) {
                                    map(joined).set(function._1, 1)
                                }
                                else {
                                    map(joined).set(function._1, map(joined).getCell(function._1) + DataCell(1, DataType.INTEGER))
                                }
                            }
                            else {
                                //单字段排重
                                if (!map(joined).contains(function._1)) {
                                    map(joined).set(function._1, 0)
                                }
                                val key = joined + ",#" + function._1
                                if (!counts.contains(key)) {
                                    map(joined).set(function._1 + "_KEY", key)
                                    counts += key -> new mutable.HashSet[String]()
                                }
                                counts(key) += row.mkString(column.split("\\+") : _*)
                            }
                        case "MIN" =>
                            if (contains(column)) {
                                if (!map(joined).contains(function._1)) {
                                    map(joined).set(function._1, row.getCell(column))
                                }
                                else if (row.getCell(column) < map(joined).getCell(function._1)) {
                                    map(joined).set(function._1, row.getCell(column))
                                }
                            }
                            else {
                                throw new TableColumnNotFoundException(s"Column name '$column' doesn't exists.")
                            }
                        case "MAX" =>
                            if (contains(column)) {
                                if (!map(joined).contains(function._1)) {
                                    map(joined).set(function._1, row.getCell(column))
                                }
                                else if (row.getCell(column) > map(joined).getCell(function._1)) {
                                    map(joined).set(function._1, row.getCell(column))
                                }
                            }
                            else {
                                throw new TableColumnNotFoundException(s"Column name '$column' doesn't exists.")
                            }
                        case "AVG" =>
                            if (contains(column)) {
                                if (!map(joined).contains(function._1)) {
                                    map(joined).set(function._1, row.getCell(column))
                                    map(joined).set(function._1 + "_SUM", 0D)
                                    map(joined).set(function._1 + "_COUNT", 0)
                                }

                                map(joined).set(function._1 + "_SUM", row.getCell(column) + map(joined).getCell(function._1))
                                map(joined).set(function._1 + "_COUNT", map(joined).getCell(function._1) + DataCell(1, DataType.INTEGER))

                            }
                            else {
                                throw new TableColumnNotFoundException(s"Column name '$column' doesn't exists.")
                            }
                    }
                })
            })

            map.foreach(item => {
                functions.foreach(function => {
                    function._2._1 match {
                        case "COUNT" =>
                            if (function._2._2 != "" && !"""^\d+$""".r.test(function._2._2)) {
                                item._2.set(function._1, counts(item._2.getString(function._1 + "_KEY")).size)
                                item._2.remove(function._1 + "_KEY")
                            }
                        case "AVG" =>
                            item._2.set(function._1, item._2.getDouble(function._1 + "_SUM") / item._2.getDouble(function._1 + "_COUNT"))
                            item._2.remove(function._1 + "_SUM")
                            item._2.remove(function._1 + "_COUNT")
                        case _ =>
                    }
                })
                table.addRow(item._2)
            })

            counts.clear()
        }
        else {
            rows.foreach(row => {
                val newRow = new DataRow()
                columns.foreach(column => {
                    newRow.set(column._1, row.getCell(column._2))
                })
                table.addRow(newRow)
            })
        }

        //当数据为空时，仍要保持数据结构
        if (table.isEmpty) {
            columns.foreach(col => {
                table.addField(col._1, this.getFieldDataType(col._2))
            })

            if (functions.nonEmpty) {
                functions.foreach(col => {
                    table.addField(col._1, DataType.DECIMAL)
                })
            }
        }

        table
    }

    //将所选字段排重并生成一个新表
    def distinct(fields: String*): DataTable = {
        val table = new DataTable()

        val columns = new mutable.LinkedHashMap[String, String]()
        val set = new mutable.HashSet[String]()
        val list = new mutable.ArrayBuffer[String]()

        fields.foreach(field => {
            if (field.equals("*")) {
                this.fields.foreach(column => {
                    columns += column -> column
                })
            }
            else {
                """(?i)\sAS\s""".r.findFirstIn(field) match {
                    case Some(as) =>
                        val origin = field.takeBefore(as).trim()
                        if (contains(origin)) {
                            columns += field -> origin
                        }
                        else {
                            throw new TableColumnNotFoundException(s"Column name '$origin' doesn't exists.")
                        }
                    case None =>
                        if (contains(field)) {
                            columns += field -> field
                        }
                        else {
                            throw new TableColumnNotFoundException(s"Column name '$field' doesn't exists.")
                        }
                }
            }
        })

        rows.foreach(row => {

            val newRow = new DataRow()

            columns.foreach(column => {
                list += row.getString(column._2)
                newRow.set(column._1, row.getCell(column._2))
            })

            val joined = list.mkString("#")
            if (!set.contains(joined)) {
                set += joined
                table.addRow(newRow)
            }

            list.clear()
        })

        //当表为空时，添加表结构
        if (table.isEmpty) {
            columns.foreach(column => {
                table.addField(column._1, this.getFieldDataType(column._2))
            })
        }

        table
    }

    //SELECT+WHERE
    def select(filter: DataRow => Boolean)(fieldNames: String*): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            if (filter(row)) {
                val newRow = new DataRow()
                fieldNames.foreach(fieldName => {
                    newRow.set(fieldName, row.getCell(fieldName))
                })
                table.addRow(newRow)
            }
        })

        if (table.isEmpty) {
            fieldNames.foreach(fieldName => {
                table.addField(fieldName, getFieldDataType(fieldName))
            })
        }

        table
    }

    // SELECT "A, B AS C"
    // SELECT "B, SUM(A) AS C HAVING C>10"
    // SELECT "A, B, GATHER(C, D, E, F) AS X"
    // SELECT "A, B, GATHER(C, D, GATHER(E, F)) AS X"
    // SELECT "*, TREE(parentId=0) AS nodes"
    // SELECT "A, B, PARTITION(C,D)"  二次分组聚合
    // 二次分组排序, TOP N, 再次聚合, 参与SQL SERVER和第一版FSQL
    // SELECT ... WHERE ... HAVING ... LIMIT N
    //def select(fragment: String): DataTable = {
        //"select value from cookies where name='name' limit 1"
    //    null
    //}

    // 删除列
    def drop(fieldNames: String*): Unit = {
        for (fieldName <- fieldNames) {
            val name = this.getColumnName(fieldName).getOrElse("")
            if (name != "") {
                fields -= name
                columns -= name
                labels -= name
                rows.foreach(_.remove(name))
            }
        }
    }

    // 修改列名
    def alter(fieldName: String, newFieldName: String): Unit = {
        val oldName = this.getColumnName(fieldName).getOrElse("")
        if (oldName != "") {
            fields(fields.indexOf(oldName)) = newFieldName
            columns += newFieldName -> columns(oldName)
            columns -= oldName
            labels += newFieldName -> { if (labels(oldName).equalsIgnoreCase(fieldName)) newFieldName else labels(oldName) }
            labels -= oldName

            rows.foreach(_.alter(fieldName, newFieldName))
        }
    }

    // 修改数据类型
    def alter(fieldName: String, dataType: DataType): Unit = {
        val name = this.getColumnName(fieldName).getOrElse("")
        if (name != "") {
            columns += name -> dataType
        }
    }

    // 修改列名 A AS A1, B AS B2
    def alter(fragment: String): DataTable = {
        fragment.split(",")
            .map(f => (f.takeBeforeX("(?i)\\sAS\\s".r).trim(), f.takeAfterX("(?i)\\sAS\\s".r).trim()))
            .filter(f => f._1 != "" && f._2 != "")
            .foreach(f => {
                alter(f._1, f._2)
            })

        this
    }

    def turnToRow: DataRow = {
        val newRow = new DataRow()
        if (this.width >= 2) {
            this.rows.foreach(row => {
                newRow.set(row.getString(0), row.getCell(1))
            })
        }
        else if (this.width == 1) {
            for (i <- 0 until this.size) {
                newRow.set("value" + i, this.rows(i).getCell(0))
            }
        }
        newRow
    }

    def turnToRow(fieldColumn: String, valueColumn: String): DataRow = {
        val newRow = new DataRow()
        this.rows.foreach(row => {
            newRow.set(row.getString(fieldColumn), row.getCell(valueColumn))
        })
        newRow
    }

    //将数据表转化成另一种形式，可以一行变多行，传入要求必须是 Json 对象数组字符串
    def trans(jsonStr: String): DataTable = {
        val params = jsonStr.pickParameters()
        val table = new DataTable()
        this.rows.foreach(row => {
            table.merge(cn.qross.net.Json(jsonStr.replaceParameters(params, row, "\"")).parseTable("/"))
        })

        if (table.isEmpty) {
            val row = new DataRow()
            this.getColumns.foreach(column => {
                row.set(column._1, column._2 match {
                    case DataType.DECIMAL | DataType.INTEGER => 0
                    case DataType.NULL => null
                    case DataType.BOOLEAN => true
                    case _ => ""
                })
            })
            cn.qross.net.Json(jsonStr.replaceParameters(params, row, "\""))
                .parseTable("/")
                .getColumns.foreach(column => {
                table.addField(column._1, column._2)
            })
        }

        table
    }

    def turnToNestedMap(fieldName: String): java.util.LinkedHashMap[String, java.util.LinkedHashMap[String, Any]] = {
        val map = new java.util.LinkedHashMap[String, java.util.LinkedHashMap[String, Any]]()
        //如果指定了错误的 fieldName, 会使用 fieldName 当 key, 结果会不正确
        this.rows.foreach(row => {
            val column = row.getString(fieldName)
            map.put(column, new java.util.LinkedHashMap[String, Any]())
            row.foreach((field, value) => {
                if (field != fieldName) {
                    map.get(column).put(field, value)
                }
            })
        })
        map
    }

    def turnToGroupedTables(fieldName: String): java.util.LinkedHashMap[String, java.util.ArrayList[java.util.LinkedHashMap[String, Any]]] = {
        val map = new java.util.LinkedHashMap[String, java.util.ArrayList[java.util.LinkedHashMap[String, Any]]]()

        this.rows.foreach(row => {
            val column = row.getString(fieldName, fieldName)
            if (!map.containsKey(column)) {
                map.put(column, new util.ArrayList[util.LinkedHashMap[String, Any]]())
            }
            val line = new util.LinkedHashMap[String, Any]()
            row.foreach((field, value) => {
                if (field != fieldName) {
                    line.put(field, value)
                }
            })
            map.get(column).add(line)
        })

        map
    }

    /**
     * 按照指定的列做为分层依据将表格转成一个树形结构
     * @param   parentColumn    父级列的列名
     * @param   startPoint      父组列起始层的值，支持整数和字符串
     * @param   newColumn       新列的 id
     */
    def turnToTree(primaryColumn: String, parentColumn: String, startPoint: String, newColumn: String): java.util.List[java.util.Map[String, Any]] = {
        //id -> data
        val relations = new mutable.HashMap[String, java.util.Map[String, Any]]()
        //[data]
        val result: java.util.List[util.Map[String, Any]] = new util.ArrayList[util.Map[String, Any]]()
        //第一次未找到的项
        val stash = new mutable.ArrayBuffer[java.util.Map[String, Any]]()

        rows.foreach(row => {
            val parent = row.getString(parentColumn)
            val map = row.toJavaMap
            map.put(newColumn, new util.ArrayList[Any]())

            relations += row.getString(primaryColumn) -> map
            if (parent == startPoint) {
                result.add(map)
            }
            else if (relations.contains(parent)) {
                relations(parent).get(newColumn).asInstanceOf[java.util.ArrayList[Any]].add(map)
            }
            else {
                //如果找不到，有可能其父级还在后面，先暂存
                stash += map
            }
        })

        //再次遍历
        stash.foreach(map => {
            val parent = map.get(parentColumn).toString
            if (relations.contains(parent)) {
                relations(parent).get(newColumn).asInstanceOf[java.util.ArrayList[Any]].add(map)
            }
            else {
                //实在找不到，就变成顶级
                result.add(map)
            }
        })

        stash.clear()
        relations.clear()
        this.clear()

        result
    }

    def collect(newColumnName: String, columns: String*): java.util.List[java.util.Map[String, Any]] = {
        val result: java.util.List[util.Map[String, Any]] = new util.ArrayList[util.Map[String, Any]]()

        rows.foreach(row => {
            val map = row.toJavaMap
            val child: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
            for (column <- columns) {
                child.put(column, map.get(column))
                map.remove(column)
            }
            map.put(newColumnName, child)
            result.add(map)
        })

        result
    }

    def updateSource(SQL: String): DataTable = {
        updateSource(JDBC.DEFAULT, SQL)
        this
    }

    def updateSource(dataSource: String, SQL: String): DataTable = {
        val ds = new DataSource(dataSource)
        ds.tableUpdate(SQL, this)
        ds.close()

        this
    }

    def batchUpdate(dataSource: DataSource, nonQuerySQL: String): Boolean = {
        if (nonEmpty) {
            dataSource.tableUpdate(nonQuerySQL, this)
            true
        }
        else {
            false
        }
    }

    def copy(otherTable: DataTable): DataTable = {
        clear()
        union(otherTable)
        this
    }

    def cut(otherTable: DataTable): DataTable = {
        clear()
        merge(otherTable)
        this
    }

    //合并并清空原表
    def merge(otherTable: DataTable): DataTable = {
        union(otherTable)
        otherTable.clear()
        this
    }

    def mergeNotExists(otherTable: DataTable): DataTable = {
        val all = rows.map(_.mkString()).toSet
        otherTable.rows.foreach(row => {
            if (!all.contains(row.mkString())) {
                addRow(row)
            }
        })
        otherTable.clear()
        this
    }

    //可不同结构的表进行结合
    def union(otherTable: DataTable): DataTable = {
        otherTable.fields.foreach(field => {
            if (!columns.contains(field)) {
                fields += field
            }
        })
        columns ++= otherTable.columns
        labels ++= otherTable.labels
        rows ++= otherTable.rows
        this
    }

    def join(otherTable: DataTable, on: (String, String)*): DataTable = {
        otherTable.fields.foreach(field => {
            if (!columns.contains(field)) {
                this.fields += field
            }
        })
        columns ++= otherTable.columns
        labels ++= otherTable.labels
        for (row <- this.rows) {
            for (line <- otherTable.rows) {
                var matched = true
                breakable {
                    for (pair <- on) {
                        if (row.getString(pair._1) != line.getString(pair._2)) {
                            matched = false
                            break
                        }
                    }
                }
                if (matched) {
                    row.combine(line)
                }
            }
        }
        otherTable.clear()
        this
    }

    def nonEmpty: Boolean = rows.nonEmpty
    def isEmpty: Boolean = rows.isEmpty
    def isEmptySchema: Boolean = fields.isEmpty
    def nonEmptySchema: Boolean = fields.nonEmpty

    def orELse(otherTable: DataTable): DataTable = {
        if (this.nonEmpty) {
            this
        }
        else {
            otherTable
        }
    }

    def getFieldNames: List[String] = fields.toList
    def getFieldNameList: java.util.List[String] = getFieldNames.asJava
    def getLabelNames: List[String] = labels.values.toList
    def getLabelNameList: java.util.List[String] = getLabelNames.asJava
    def getLabels: mutable.LinkedHashMap[String, String] = labels
    def getHeaders: DataRow = {
        val list = new DataRow()
        labels.foreach(kv => {
            list.set(kv._1, kv._2, DataType.TEXT)
        })
        list
    }
    def getColumns: mutable.LinkedHashMap[String, DataType] = columns
    def getFieldDataType(fieldName: String): DataType = {
        val name = this.getColumnName(fieldName).getOrElse("")
        if (name != "") {
            this.columns(name)
        }
        else {
            DataType.NULL
        }
    }
    def getRow(i: Int): Option[DataRow] = if (i < rows.size) Some(rows(i)) else None
    def getRowList: java.util.List[DataRow] = rows.asJava
    def getColumn(fieldName: String): List[Any] = rows.map(row => row.get(fieldName).orNull).toList

    def firstRow: Option[DataRow] = if (rows.nonEmpty) Some(this.rows(0)) else None
    def lastRow: Option[DataRow] = if (rows.nonEmpty) Some(this.rows(this.rows.size - 1)) else None

    def firstColumn: Option[List[Any]] = {
        if (this.fields.nonEmpty) {
            Some(this.rows.map(row => row.get(0).orNull).toList)
        }
        else {
            None
        }
    }
    def lastColumn: Option[List[Any]] = {
        if (this.fields.nonEmpty) {
            Some(this.rows.map(row => row.get(fields.size - 1).orNull).toList)
        }
        else {
            None
        }
    }

    def getCell(rowIndex: Int, colIndex: Int): DataCell = {
        getRow(rowIndex) match {
            case Some(row) => row.getCell(colIndex)
            case None => DataCell.UNDEFINED
        }
    }

    def getCell(rowIndex: Int, fieldName: String): DataCell = {
        getRow(rowIndex) match {
            case Some(row) => row.getCell(fieldName)
            case None => DataCell.UNDEFINED
        }
    }

    def getFirstCellStringValue(defaultValue: String = ""): String = {
        firstRow match {
            case Some(row) => row.getString(0, defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellIntValue(defaultValue: Int = 0): Int = {
        firstRow match {
            case Some(row) => row.getInt(0, defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellLongValue(defaultValue: Long = 0L): Long = {
        firstRow match {
            case Some(row) => row.getLong(0, defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellFloatValue(defaultValue: Float = 0F): Float = {
        firstRow match {
            case Some(row) => row.getFloat(0, defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellDoubleValue(defaultValue: Double = 0D): Double = {
        firstRow match {
            case Some(row) => row.getDouble(0, defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellBooleanValue(defaultValue: Boolean = false): Boolean = {
        firstRow match {
            case Some(row) => row.getBoolean(0, defaultValue)
            case None => defaultValue
        }
    }

    def firstCellStringValue: String = getFirstCellStringValue()
    def firstCellIntValue: Int = getFirstCellIntValue()
    def firstCellLongValue: Long = getFirstCellLongValue()
    def firstCellFloatValue: Float = getFirstCellFloatValue()
    def firstCellDoubleValue: Double = getFirstCellDoubleValue()
    def firstCellBooleanValue: Boolean = getFirstCellBooleanValue()

    def show(limit: Int = 20): Unit = {
        Output.writeLine("------------------------------------------------------------------------")
        Output.writeLine(rows.size, " rows")
        Output.writeLine("------------------------------------------------------------------------")
        Output.writeLine(getLabelNames.mkString(", "))
        breakable {
            var i = 0
            for (row <- rows) {
                Output.writeLine(row.mkString(", "))
                i += 1
                if (i >= limit) {
                    break
                }
            }
        }
        Output.writeLine("------------------------------------------------------------------------")
    }

    def toJavaMapList: java.util.List[java.util.Map[String, Any]] = {
        rows.map(row => row.values.asJava).asJava
    }

    def toList[T]: List[T] = {
        rows.map(row => row.values.head._2.asInstanceOf[T]).toList
    }

    def toJavaList[T]: java.util.List[T] = {
        rows.map(row => row.values.head._2.asInstanceOf[T]).asJava
    }

    def toHashSet[T]: java.util.Set[T] = {
        rows.map(row => row.values.head._2.asInstanceOf[T]).toSet.asJava
    }

    def toJsonString: String = {
        val sb = new StringBuilder()
        for ((fieldName, dataType) <- columns) {
            if (sb.nonEmpty) {
                sb.append(",")
            }
            sb.append("\"" + fieldName + "\":\"" + dataType + "\"")
        }
        "{\"columns\":{" + sb.toString +"}, \"rows\":" + rows.asJava.toString + "}"
    }

    override def toString: String = {
        //Json.serialize(toJavaMapList)
        new ObjectMapper().writeValueAsString(toJavaMapList)
    }
    
    def toHtmlString(limit: Int = 0): String = {
        val sb = new StringBuilder()
        sb.append("""<table datatable="yes" cellpadding="5" cellspacing="1" border="0" style="background-color:#909090">""")
        sb.append("<tr>")
        columns.keySet.foreach(field => {
            sb.append("""<th style="text-align: left; background-color:#D0D0D0">""")
            sb.append(labels(field))
            sb.append("</th>")
        })
        sb.append("</tr>")
        breakable {
            var sum = 0
            for (row <- rows) {
                sb.append("<tr>")
                row.getFields.foreach(field => {
                    sb.append("""<td style="background-color: #FFFFFF;""")
                    row.getDataType(field) match {
                        case Some(dt) => if (dt == DataType.DECIMAL || dt == DataType.INTEGER) sb.append(" text-align: right;")
                        case _ =>
                    }
                    sb.append("""">""")
                    sb.append(row.getString(field))
                    sb.append("</td>")
                })
                sb.append("</tr>")

                if (limit > 0) {
                    sum += 1
                    if (sum >= limit) {
                        break
                    }
                }
            }
        }
        sb.append("</table>")
        
        sb.toString()
    }
    
    def clear(): Unit = {
        this.rows.clear()
        this.columns.clear()
        this.labels.clear()
        this.fields.clear()
    }
}
