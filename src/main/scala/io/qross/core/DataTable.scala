package io.qross.core

import com.fasterxml.jackson.databind.ObjectMapper
import DataType.DataType
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.ext.Output
import io.qross.ext.TypeExt._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray
import scala.util.Random
import scala.util.control.Breaks._

object DataTable {
    
    def from(dataTable: DataTable): DataTable = {
        new DataTable().copy(dataTable)
    }
    
    def ofSchema(dataTable: DataTable): DataTable = {
        val table = new DataTable()
        table.fields ++= dataTable.fields
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
    
    val rows = new mutable.ArrayBuffer[DataRow]()
    private val fields = new mutable.LinkedHashMap[String, DataType]()
    private val labels = new mutable.LinkedHashMap[String, String]()


    def addField(fieldName: String, dataType: DataType): Unit = {
        addFieldWithLabel(fieldName, fieldName, dataType)
    }

    def addFieldWithLabel(fieldName: String, labelName: String, dataType: DataType): Unit = {
        // . is illegal char in SQLite
        val name = { if (fieldName.contains(".")) fieldName.takeAfter(".") else fieldName }.toLowerCase()
        fields += name -> dataType
        labels += name -> labelName
    }
    
    def label(alias: (String, String)*): DataTable = {
        for ((fieldName, otherName) <- alias) {
            labels += fieldName.toLowerCase() -> otherName
        }
        this
    }
    
    def contains(fieldName: String): Boolean = fields.contains(fieldName.toLowerCase())

    def newRow(): DataRow = {
        val row = new DataRow()
        row.fields ++= fields
        row.table = this
        row
    }

    //添加的行结构需要与table一致
    def insert(row: DataRow): Unit = {
        rows += row
    }

    def alter(fieldName: String, newName: String): Unit = {
        val name = fieldName.toLowerCase()
        if (fields.contains(name)) {
            val list = fields.toList
            fields.clear()
            list.foreach(item => {
                if (item._1 != name) {
                    fields += item._1 -> item._2
                }
                else {
                    fields += newName -> item._2
                }
            })
        }
    }

    def alter(fieldName: String, dataType: DataType): Unit = {
        val name = fieldName.toLowerCase()
        if (fields.contains(name)) {
            fields += name -> dataType
        }
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

    def par: ParArray[DataRow] = {
        rows.par
    }
    
    def foreach(callback: DataRow => Unit): DataTable = {
        rows.foreach(row => {
            callback(row)
        })
        this
    }

    //遍历并返回新的DataTable
    def iterate(callback: DataRow => Unit): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            callback(row)
            table.addRow(row)
        })

        table
    }
    
    def collect(filter: DataRow => Boolean) (map: DataRow => DataRow): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            if (filter(row)) {
                table.addRow(map(row))
            }
        })

        table
    }
    
    def map(callback: DataRow => DataRow): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            table.addRow(callback(row))
        })

        table
    }
    
    def table(fields: (String, DataType)*)(callback: DataRow => DataTable): DataTable = {
        val table = DataTable.withFields(fields: _*)
        rows.foreach(row => {
            table.merge(callback(row))
        })

        table
    }
    
    def filter(callback: DataRow => Boolean): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            if (callback(row)) {
                table.addRow(row)
            }
        })

        table
    }
    
    def size: Int = rows.size
    def count(): Int = rows.size
    def columnCount: Int = fields.size
    
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
    
    //avg
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
    
    //max
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
    
    //min
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
    
    //take
    def take(amount: Int): DataTable = {
        val table = new DataTable()
        for (i <- 0 until amount) {
            table.addRow(rows(i))
        }
        
        table
    }

    def takeSample(amount: Int): DataTable = {
        val table = new DataTable()
        Random.shuffle(rows)
            .take(amount)
            .foreach(row => {
                table.addRow(row)
            })

        table
    }

    def insertRow(fields: (String, Any)*): DataTable = {
        addRow(new DataRow(fields: _*))
        this
    }
    
    def updateWhile(filter: DataRow => Boolean)(setValue: DataRow => Unit): DataTable = {
        rows.foreach(row => {
            if (filter(row)) {
                setValue(row)
            }
        })
        this
    }
    
    def upsertRow(filter: DataRow => Boolean)(setValue: DataRow => Unit)(fields: (String, Any)*): DataTable = {
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
            insertRow(fields: _*)
        }
        
        this
    }
    
    def deleteWhile(filter: DataRow => Boolean): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            if (!filter(row)) {
                table.addRow(row)
            }
        })
        clear()
        table
    }
    
    def select(filter: DataRow => Boolean)(fieldNames: String*): DataTable = {
        val table = new DataTable()
        rows.foreach(row => {
            if (filter(row)) {
                val newRow = new DataRow()
                fieldNames.foreach(fieldName => {
                    newRow.set(fieldName, row.get(fieldName).orNull)
                })
                table.addRow(newRow)
            }
        })
        table
    }

    def select(filter: DataRow => Boolean): ArrayBuffer[DataRow] = {
        val rows = new ArrayBuffer[DataRow]()
        this.rows.foreach(row => {
            if (filter(row)) {
                rows += row
            }
        })
        rows
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

    def nonEmpty: Boolean = {
        rows.nonEmpty
    }
    
    def isEmpty: Boolean = {
        rows.isEmpty
    }

    def isEmptySchema: Boolean = {
        fields.isEmpty
    }

    def nonEmptySchema: Boolean = {
        fields.nonEmpty
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
    
    def union(otherTable: DataTable): DataTable = {
        fields ++= otherTable.fields
        labels ++= otherTable.labels
        rows ++= otherTable.rows
        this
    }

    def join(otherTable: DataTable, on: (String, String)*): DataTable = {
        fields ++= otherTable.fields
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

    def batchUpdate(dataSource: DataSource, nonQuerySQL: String): Boolean = {
        if (nonEmpty) {
            dataSource.tableUpdate(nonQuerySQL, this)
            true
        }
        else {
            false
        }
    }
    
    def getFieldNames: List[String] = fields.keySet.toList
    def getFieldNameList: java.util.List[String] = getFieldNames.asJava
    def getLabelNames: List[String] = labels.values.toList
    def getLabelList: java.util.List[String] = getLabelNames.asJava
    def getLabels: mutable.LinkedHashMap[String, String] = labels
    def getFields: mutable.LinkedHashMap[String, DataType] = fields
    def getFieldType(fieldName: String): DataType = fields(fieldName.toLowerCase())
    def getRow(i: Int): Option[DataRow] = if (i < rows.size) Some(rows(i)) else None
    def getRowList: java.util.List[DataRow] = rows.asJava
    def getColumn(fieldName: String): List[Any] = rows.map(row => row.columns(fieldName.toLowerCase())).toList

    def firstRow: Option[DataRow] = if (rows.nonEmpty) Some(rows(0)) else None
    def lastRow: Option[DataRow] = if (rows.nonEmpty) Some(rows(rows.size - 1)) else None

    def firstColumn: Option[List[Any]] = if (fields.nonEmpty) Option(rows.map(r => r.columns.head._2).toList) else None
    def lastColumn: Option[List[Any]] = if (fields.nonEmpty) Option(rows.map(r => r.columns.last._2).toList) else None

    def getFirstCellStringValue(defaultValue: String = ""): String = {
        firstRow match {
            case Some(row) => row.getFirstString(defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellIntValue(defaultValue: Int = 0): Int = {
        firstRow match {
            case Some(row) => row.getFirstInt(defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellLongValue(defaultValue: Long = 0L): Long = {
        firstRow match {
            case Some(row) => row.getFirstLong(defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellFloatValue(defaultValue: Float = 0F): Float = {
        firstRow match {
            case Some(row) => row.getFirstFloat(defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellDoubleValue(defaultValue: Double = 0D): Double = {
        firstRow match {
            case Some(row) => row.getFirstDouble(defaultValue)
            case None => defaultValue
        }
    }

    def getFirstCellBooleanValue(defaultValue: Boolean = false): Boolean = {
        firstRow match {
            case Some(row) => row.getFirstBoolean(defaultValue)
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
                Output.writeLine(row.join(", "))
                i += 1
                if (i >= limit) {
                    break
                }
            }
        }
        Output.writeLine("------------------------------------------------------------------------")
    }

    def toJavaMapList: java.util.List[java.util.Map[String, Any]] = {
        rows.map(row => row.columns.asJava).asJava
    }

    def toList: List[Any] = {
        rows.map(row => row.columns.head._2).toList
    }

    def toJavaList: java.util.List[Any] = {
        toList.asJava
    }

    def toJsonString: String = {
        val sb = new StringBuilder()
        for ((fieldName, dataType) <- fields) {
            if (sb.nonEmpty) {
                sb.append(",")
            }
            sb.append("\"" + fieldName + "\":\"" + dataType + "\"")
        }
        "{\"fields\":{" + sb.toString +"}, \"rows\":" + rows.asJava.toString + "}"
    }

    override def toString: String = {
        new ObjectMapper().writeValueAsString(toJavaMapList)
    }
    
    def toHtmlString: String = {
        val sb = new StringBuilder()
        sb.append("""<table cellpadding="5" cellspacing="1" border="0" style="background-color:#909090">""")
        sb.append("<tr>")
        fields.keySet.foreach(field => {
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
        fields.clear()
        labels.clear()
    }
}
