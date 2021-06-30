package io.qross.core

import java.util.Objects

import com.fasterxml.jackson.databind.ObjectMapper
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.time.DateTime

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

object DataRow {
   
    def from(json: String): DataRow = Json(json).parseRow("/")
    def from(json: String, path: String): DataRow = Json(json).parseRow(path)
    def from(row: DataRow, fieldNames: String*): DataRow = {
        val newRow = new DataRow()
        if (fieldNames.nonEmpty) {
            fieldNames.foreach(fieldName => newRow.set(fieldName, row.getCell(fieldName)))
        }
        else {
            newRow.fields ++= row.fields
            newRow.values ++= row.values
            newRow.columns ++= row.columns
        }
        newRow
    }
}

class DataRow() {

    def this(items: (String, Any)*) {
        this()

        for ((k, v) <- items) {
            set(k, v)
        }
    }

    val fields = new mutable.ArrayBuffer[String]() //字段名索引列表
    val columns = new mutable.LinkedHashMap[String, DataType]() //列名及数据类型
    val values = new mutable.LinkedHashMap[String, Any]()   //列名及值

    //insert or update
    def set(fieldName: String, value: Any): Unit = {
        set(fieldName, value, DataType.ofValue(value))
    }

     def set(fieldName: String, cell: DataCell): Unit = {
        set(fieldName, cell.value, cell.dataType)
    }

    def set(fieldName: String, value: Any, dataType: DataType): Unit = {

        if (!columns.contains(fieldName)) {
            //不存在则添加列
            fields += fieldName
            columns += fieldName -> dataType
        }
        else if (columns(fieldName) != dataType && dataType != DataType.NULL) {
            //仅修改数据类型
            columns += fieldName -> dataType
        }

        this.values += fieldName -> {
            if (value != null) {
                dataType match {
                    case DataType.DATETIME => value.toDateTime.toString
                    case DataType.BLOB => new String(value.asInstanceOf[Array[Byte]])
                    //case DataType.JSON => value
                    case _ => value
                }
            }
            else {
                null
            }
        }
    }

    def remove(fieldName: String): Unit = {
        val name = getFieldName(fieldName).getOrElse("")
        if (name != "") {
            this.fields -= name
            this.columns -= name
            this.values -= name
        }
    }

    def updateFieldName(fieldName: String, newFieldName: String): Unit = {
        val name = getFieldName(fieldName).getOrElse("")
        if (name != "") {
            this.fields(this.fields.indexOf(name)) = newFieldName
            this.values += newFieldName -> this.values(name)
            this.values -= name
            this.columns += newFieldName -> this.columns(name)
            this.columns -= name
        }
    }

    def alter(fieldName: String, newFieldName: String): Unit = {
        updateFieldName(fieldName, newFieldName)
    }

    def foreach(callback: (String, Any) => Unit): Unit = {
        for ((k, v) <- values) {
            callback(k, v)
        }
    }

    def get(fieldName: String): Option[Any] = {
        getFieldName(fieldName) match {
            case Some(name) => values.get(name)
            case None => None
        }
    }

    //by index
    def get(index: Int): Option[Any] = {
        if (index < values.size) {
            values.get(fields(index))
        }
        else {
            None
        }
    }

    def getCell(fieldName: String): DataCell = {
        val name = this.getFieldName(fieldName).getOrElse("")
        if (name != "") {
            DataCell(this.values(name), this.columns(name))
        }
        else {
            DataCell.UNDEFINED
        }
    }

    def getCell(index: Int): DataCell = {
        if (index < this.values.size) {
            val name = this.fields(index)
            DataCell(this.values(name), this.columns(name))
        }
        else {
            DataCell.UNDEFINED
        }
    }

    def firstCell: DataCell = {
        getCell(0)
    }

    def lastCell: DataCell = {
        if (this.fields.nonEmpty) {
            getCell(this.fields.size - 1)
        }
        else {
            DataCell.UNDEFINED
        }
    }

    def getJson(fieldName: String): Json = {
        val name = this.getFieldName(fieldName).getOrElse("")
        if (name != "") {
            this.values(name).asInstanceOf[Json]
        }
        else {
            Json()
        }
    }

    def string(fieldName: String): String = getString(fieldName, "")
    def string(index: Int): String = getString(index, "")
    def getString(fieldName: String): String = getString(fieldName, "")
    def getString(index: Int): String = getString(index, "")
    def getString(fieldName: String, defaultValue: String): String = getStringOption(fieldName).getOrElse(defaultValue)
    def getString(index: Int, defaultValue: String): String = getStringOption(index).getOrElse(defaultValue)
    def getStringOption(fieldName: String): Option[String] = {
        get(fieldName) match {
            case Some(value) => if (value != null) Some(value.toString) else Some(null)
            case None => None
        }
    }
    def getStringOption(index: Int): Option[String] = {
        if (index < fields.size) {
            getStringOption(fields(index))
        }
        else {
            None
        }
    }

    def int(fieldName: String): Int = getInt(fieldName, 0)
    def int(index: Int): Int = getInt(index, 0)
    def getInt(fieldName: String): Int = getInt(fieldName, 0)
    def getInt(index: Int): Int = getInt(index, 0)
    def getInt(fieldName: String, defaultValue: Int): Int = getIntOption(fieldName).getOrElse(defaultValue)
    def getInt(index: Int, defaultValue: Int): Int = getIntIntOption(index).getOrElse(defaultValue)
    def getIntOption(fieldName: String): Option[Int] = {
        get(fieldName) match {
            case Some(value) => value match {
                    case v: Int => Some(v)
                    case other => Try(other.toString.toDouble.toInt) match {
                            case Success(v) => Some(v)
                            case Failure(_) => None
                        }
                }
            case None => None
        }
    }
    def getIntIntOption(index: Int): Option[Int] = {
        if (index < fields.size) {
            getIntOption(fields(index))
        }
        else {
            None
        }
    }

    def long(fieldName: String): Long = getLong(fieldName, 0L)
    def long(index: Int): Long = getLong(index, 0L)
    def getLong(fieldName: String): Long = getLong(fieldName, 0L)
    def getLong(index: Int): Long = getLong(index, 0L)
    def getLong(fieldName: String, defaultValue: Long): Long = getLongOption(fieldName).getOrElse(defaultValue)
    def getLong(index: Int, defaultValue: Long): Long = getLongOption(index).getOrElse(defaultValue)
    def getLongOption(fieldName: String): Option[Long] = {
        get(fieldName) match {
            case Some(value) => value match {
                case v: Int => Some(v)
                case v: Long => Some(v)
                case other => Try(other.toString.toDouble.toLong) match {
                    case Success(v) => Some(v)
                    case Failure(_) => None
                }
            }
            case None => None
        }
    }
    def getLongOption(index: Int): Option[Long] = {
        if (index < fields.size) {
            getLongOption(fields(index))
        }
        else {
            None
        }
    }

    def float(fieldName: String): Float = getFloat(fieldName, 0F)
    def float(index: Int): Float = getFloat(index, 0F)
    def getFloat(fieldName: String): Float = getFloat(fieldName, 0F)
    def getFloat(index: Int): Float = getFloat(index, 0F)
    def getFloat(fieldName: String, defaultValue: Float): Float = getFloatOption(fieldName).getOrElse(defaultValue)
    def getFloat(index: Int, defaultValue: Float): Float = getFloatOption(index).getOrElse(defaultValue)
    def getFloatOption(fieldName: String): Option[Float] = {
        get(fieldName) match {
            case Some(value) => value match {
                case v: Int => Some(v)
                case v: Float => Some(v)
                case other => Try(other.toString.toFloat) match {
                    case Success(v) => Some(v)
                    case Failure(_) => None
                }
            }
            case None => None
        }
    }
    def getFloatOption(index: Int): Option[Float] = {
        if (index < fields.size) {
            getFloatOption(fields(index))
        }
        else {
            None
        }
    }

    def double(fieldName: String): Double = getDouble(fieldName, 0D)
    def double(index: Int): Double = getDouble(index, 0D)
    def getDouble(fieldName: String): Double = getDouble(fieldName, 0D)
    def getDouble(index: Int): Double = getDouble(index, 0D)
    def getDouble(fieldName: String, defaultValue: Double): Double = getDoubleOption(fieldName).getOrElse(defaultValue)
    def getDouble(index: Int, defaultValue: Double): Double = getDoubleOption(index).getOrElse(defaultValue)
    def getDoubleOption(fieldName: String): Option[Double] = {
        get(fieldName) match {
            case Some(value) => value match {
                case v: Int => Some(v)
                case v: Long => Some(v)
                case v: Float => Some(v)
                case v: Double => Some(v)
                case other => Try(other.toString.toDouble) match {
                    case Success(v) => Some(v)
                    case Failure(_) => None
                }
            }
            case None => None
        }
    }
    def getDoubleOption(index: Int): Option[Double] = {
        if (index < fields.size) {
            getDoubleOption(fields(index))
        }
        else {
            None
        }
    }

    def bool(fieldName: String): Boolean = getBoolean(fieldName, false)
    def bool(index: Int): Boolean = getBoolean(index, false)
    def getBoolean(fieldName: String): Boolean = getBoolean(fieldName, false)
    def getBoolean(fieldName: String, defaultValue: Boolean): Boolean = {
        get(fieldName) match {
            case Some(value) =>
                value match {
                    case bool: Boolean => bool
                    case _ =>
                        val value = getString(fieldName, defaultValue.toString).toLowerCase
                        value == "yes" || value == "true" || value == "1" || value == "ok"
                }
            case None => defaultValue
        }
    }
    def getBoolean(index: Int): Boolean = getBoolean(index, false)
    def getBoolean(index: Int, defaultValue: Boolean): Boolean = {
        if (index < fields.size) {
            getBoolean(fields(index), defaultValue)
        }
        else {
            defaultValue
        }
    }

    def dateTime(fieldName: String): DateTime = getDateTime(fieldName, DateTime.of(1970, 1, 1))
    def dateTime(index: Int): DateTime = getDateTime(index, DateTime.of(1970, 1, 1))
    def getDateTime(fieldName: String): DateTime = getDateTime(fieldName, DateTime.of(1970, 1, 1))
    def getDateTime(index: Int): DateTime = getDateTime(index, DateTime.of(1970, 1, 1))
    def getDateTime(fieldName: String, defaultValue: DateTime): DateTime = getDateTimeOption(fieldName).getOrElse(defaultValue)
    def getDateTime(index: Int, defaultValue: DateTime): DateTime = getDateTimeOption(index).getOrElse(defaultValue)
    def getDateTimeOption(fieldName: String): Option[DateTime] = {
        get(fieldName) match {
            case Some(value) =>
                value match {
                    case dt: DateTime => Some(dt)
                    case _ => Some(new DateTime(value))
                }
            case None => None
        }
    }
    def getDateTimeOption(index: Int): Option[DateTime] = {
        if (index < fields.size) {
            getDateTimeOption(fields(index))
        }
        else {
            None
        }
    }

    def getRow(fields: String*): DataRow = {
        val row = new DataRow()
        fields.foreach(field => {
            row.set(field, getCell(field))
        })
        row
    }

    def getFields: List[String] = fields.toList
    def getDataTypes: List[DataType] = columns.values.toList
    def getValues[T]: List[T] = values.values.map(_.asInstanceOf[T]).toList
    def getFieldName(fieldName: String): Option[String] = {
        if (columns.contains(fieldName)) {
            Some(fieldName)
        }
        else {
            var name = ""
            breakable {
                for (field <- fields) {
                    if (field.equalsIgnoreCase(fieldName)) {
                        name = field
                        break
                    }
                }
            }

            if (name != "") Some(name) else None
        }
    }
    def getFieldName(index: Int): Option[String] = fields.lift(index) //if (index < fields.length) Some(fields(index)) else None
    def getDataType(index: Int): Option[DataType] = if (index < fields.length) Some(columns(fields(index))) else None
    def getDataType(fieldName: String): Option[DataType] = {
        val name = this.getFieldName(fieldName).getOrElse("")
        if (name != "") {
            this.columns.get(name)
        }
        else {
            None
        }
    }
    def contains(fieldName: String): Boolean = getFieldName(fieldName).nonEmpty
    def contains(fieldName: String, value: Any): Boolean = {
        this.contains(fieldName) && this.getString(fieldName) == value.toString
    }
    def size: Int = fields.size
    def length: Int = fields.length
    def isEmpty: Boolean = fields.isEmpty
    def nonEmpty: Boolean = fields.nonEmpty

    def combine(queryOrJsonString: String): DataRow = {
        if (queryOrJsonString != null) {
            if (queryOrJsonString.bracketsWith("{", "}")) {
                combine(Json.fromText(queryOrJsonString).parseRow("/"))
            }
            else if (queryOrJsonString.contains("=")) {
                for ((key, value) <- queryOrJsonString.splitToMap()) {
                    set(key, value, DataType.TEXT)
                }
            }
        }
        this
    }

    def combine(otherRow: DataRow): DataRow = {
        for (field <- otherRow.fields) {
            set(field, otherRow.values(field), otherRow.columns(field))
        }
        this
    }

    def orElse(otherRow: DataRow): DataRow = {
        if (this.isEmpty) {
            otherRow
        }
        else {
            this
        }
    }

    def mkString(delimiter: String = ","): String = {
        val values = new mutable.StringBuilder()
        for (field <- fields) {
            if (values.nonEmpty) {
                values.append(delimiter)
            }
            values.append(this.getString(field, "null"))
        }
        values.toString()
    }

    def join(delimiter: String = "&", terminator: String = "="): String = {
        val values = new mutable.StringBuilder()
        for (field <- fields) {
            if (values.nonEmpty) {
                values.append(delimiter)
            }
            values.append(field)
            values.append(terminator)
            values.append(this.getString(field, "null"))
        }
        values.toString()
    }

    def toMap: Map[String, Any] = values.toMap
    def toSeq: Seq[(String, Any)] = values.toSeq
    def toJavaMap: java.util.Map[String, Any] = values.asJava
    def toJavaList: java.util.List[Any] = values.values.toList.asJava
    def toHashSet: java.util.Set[Any] = values.values.toSet.asJava

    def asTable: DataTable = {
        new DataTable(this)
    }

    def turnToTable(fieldColumn: String = "key", valueColumn: String = "value"): DataTable = {
        val table = new DataTable()
        this.values.foreach(field => {
            table.addRow(
                new DataRow(
                    fieldColumn -> field._1,
                    valueColumn -> field._2
                )
            )
        })
        table
    }

    override def toString: String = {
        //Json.serialize(toJavaMap)
        new ObjectMapper().writeValueAsString(values.asJava)
    }

    override def equals(obj: scala.Any): Boolean = {
        this.values == obj.asInstanceOf[DataRow].values
    }

    override def hashCode(): Int = {
        Objects.hash(values, columns)
    }

    def clear(): Unit = {
        this.fields.clear()
        this.values.clear()
        this.columns.clear()
    }
}