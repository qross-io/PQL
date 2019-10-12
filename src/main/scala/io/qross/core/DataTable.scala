package io.qross.core

import com.fasterxml.jackson.databind.ObjectMapper
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.fql.Fragment
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.net.Json
import io.qross.pql.SQLParseException

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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

    //所有field的集合, 有序有索引
    private val fields = new mutable.ListBuffer[String]()
    //所有field及dataType的集合
    private val columns = new mutable.LinkedHashMap[String, DataType]()
    //所有field及label的集合
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
        val name = { if (fieldName.contains(".")) fieldName.takeAfter(".") else fieldName }.toLowerCase()
        if (!columns.contains(name)) {
            fields += name
            columns += name -> dataType
            labels += name -> labelName
        }
    }

    //设置标签
    def label(alias: (String, String)*): DataTable = {
        for ((fieldName, aliaName) <- alias) {
            labels += fieldName.toLowerCase() -> aliaName
        }
        this
    }

    def newRow(): DataRow = {
        val row = new DataRow()
        row.fields ++= this.fields
        row.columns ++= this.columns
        row
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

    //并行
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

    //遍历并返回新的DataTable, 数据表内容不变
    def iterate(callback: DataRow => Unit): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            callback(row)
            table.addRow(row)
        })

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

        table
    }

    def filterNot(callback: DataRow => Boolean): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            if (!callback(row)) {
                table.addRow(row)
            }
        })

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


    //是否包含某一列
    def contains(fieldName: String): Boolean = columns.contains(fieldName.toLowerCase())
    //行数
    def size: Int = rows.size
    //行数
    def height: Int = rows.size
    //行数
    def count(): Int = rows.size
    //列数
    def columnCount: Int = columns.size
    //列数
    def width: Int = columns.size

    //聚合方法
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

    //聚合方法
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
    
    //聚合方法
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

    //聚合方法
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

    //聚合方法
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
        
        table
    }

    //随机取n行
    def takeSample(amount: Int): DataTable = {
        val table = new DataTable()
        Random.shuffle(rows)
            .take(amount)
            .foreach(row => {
                table.addRow(row)
            })

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
        clear()
        table
    }

    //按短语句删除 WHERE A=1 AND B='2' , WHERE关键词可忽略
    def delete(fragment: String): DataTable = {
        null
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
        null
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
    def select(fieldNames: String*): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            val newRow = new DataRow()
            fieldNames.foreach(fieldName => {
                if (contains(fieldName)) {
                    newRow.set(fieldName, row.getCell(fieldName))
                }
            })
            table.addRow(newRow)
        })
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
    def select(fragment: String): DataTable = {
        null
    }

    // 删除列
    def drop(fieldNames: String*): Unit = {
        for (fieldName <- fieldNames) {
            val name = fieldName.toLowerCase
            fields -= name
            columns -= name
            labels -= name
            rows.foreach(_.remove(name))
        }
    }

    // 修改列名
    def alter(fieldName: String, newFieldName: String): Unit = {
        val oldName = fieldName.toLowerCase()
        val newName = newFieldName.toLowerCase()

        if (columns.contains(oldName)) {
            fields(fields.indexOf(oldName)) = newName
            columns += (newName -> columns(oldName))
            columns -= oldName
            labels += (newName -> { if (labels(oldName).equalsIgnoreCase(fieldName)) newFieldName else labels(oldName) })
            labels -= oldName

            rows.foreach(_.alter(fieldName, newFieldName))
        }
    }

    // 修改数据类型
    def alter(fieldName: String, dataType: DataType): Unit = {
        val name = fieldName.toLowerCase()
        if (columns.contains(name)) {
            columns += name -> dataType
        }
    }

    // 修改列名 A AS A1, B AS B2
    def alter(fragment: String): DataTable = {
        fragment.split(",")
            .map(f => (f.takeBefore("(?i)\\sAS\\s".r).trim(), f.takeAfter("(?i)\\sAS\\s".r).trim()))
            .filter(f => f._1 != "" && f._2 != "")
            .foreach(f => {
                alter(f._1, f._2)
            })

        this
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

    def merge(otherTable: DataTable): DataTable = {
        union(otherTable)
        otherTable.clear()
        this
    }

    //可不同结构的表进行结合
    def union(otherTable: DataTable): DataTable = {
        otherTable.fields.foreach(field => {
            if (!columns.contains(field)) {
                this.fields += field
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

    def getFieldNames: List[String] = fields.toList
    def getFieldNameList: java.util.List[String] = getFieldNames.asJava
    def getLabelNames: List[String] = labels.values.toList
    def getLabelList: java.util.List[String] = getLabelNames.asJava
    def getLabels: mutable.LinkedHashMap[String, String] = labels
    def getColumns: mutable.LinkedHashMap[String, DataType] = columns
    def getFieldDataType(fieldName: String): DataType = columns(fieldName.toLowerCase())
    def getRow(i: Int): Option[DataRow] = if (i < rows.size) Some(rows(i)) else None
    def getRowList: java.util.List[DataRow] = rows.asJava
    def getColumn(fieldName: String): List[Any] = rows.map(row => row.get(fieldName).orNull).toList

    def firstRow: Option[DataRow] = if (rows.nonEmpty) Some(rows(0)) else None
    def lastRow: Option[DataRow] = if (rows.nonEmpty) Some(rows(rows.size - 1)) else None

    def firstColumn: Option[List[Any]] = {
        if (fields.nonEmpty) {
            Some(rows.map(row => row.get(0).orNull).toList)
        }
        else {
            None
        }
    }
    def lastColumn: Option[List[Any]] = {
        if (fields.nonEmpty) {
            Some(rows.map(row => row.get(fields.size - 1).orNull).toList)
        }
        else {
            None
        }
    }

    def getCell(rowIndex: Int, colIndex: Int): DataCell = {
        getRow(rowIndex) match {
            case Some(row) => row.getCell(colIndex)
            case None => DataCell.NOT_FOUND
        }
    }

    def getCell(rowIndex: Int, fieldName: String): DataCell = {
        getRow(rowIndex) match {
            case Some(row) => row.getCell(fieldName)
            case None => DataCell.NOT_FOUND
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
        Output.writeLine(rows.size, " ROWS")
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
        toList[T].asJava
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
    
    def toHtmlString: String = {
        val sb = new StringBuilder()
        sb.append("""<table cellpadding="5" cellspacing="1" border="0" style="background-color:#909090">""")
        sb.append("<tr>")
        columns.keySet.foreach(field => {
            sb.append("""<th style="text-align: left; background-color:#D0D0D0">""")
            sb.append(labels(field))
            sb.append("</th>")
        })
        sb.append("</tr>")
        rows.foreach(row => {
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
        })
        sb.append("</table>")
        
        sb.toString()
    }
    
    def clear(): Unit = {
        rows.clear()
        columns.clear()
        labels.clear()
        fields.clear()
    }
}
