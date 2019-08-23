package io.qross.core

import java.util.Objects

import com.fasterxml.jackson.databind.ObjectMapper
import DataType.DataType
import io.qross.net.Json
import io.qross.time.DateTime
import io.qross.ext.TypeExt._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object DataRow {
   
    def from(json: String): DataRow = Json(json).parseRow("/")
    def from(json: String, path: String): DataRow = Json(json).parseRow(path)
    def from(row: DataRow, fieldNames: String*): DataRow = {
        val newRow = new DataRow()
        if (fieldNames.nonEmpty) {
            fieldNames.foreach(fieldName => newRow.set(fieldName, row.get(fieldName).get))
        }
        else {
            newRow.columns ++= row.columns
            newRow.fields ++= row.fields
        }
        newRow
    }
}

class DataRow() {

    def this(items: (String, Any)*) {
        this()

        for ((k, v) <- items) {
            this.set(k, v)
        }
    }
    
    val columns = new mutable.LinkedHashMap[String, Any]()
    val fields = new mutable.LinkedHashMap[String, DataType]()
    var table: DataTable = _  // 所属的DataTable

    //insert or update
    def set(fieldName: String, value: Any): Unit = {
        if (table == null) {
            //独立row
            set(fieldName, value, DataType.ofValue(value))
        }
        else {
            //在table中的row
            if (table.contains(fieldName)) {
                set(fieldName, value, table.getFieldType(fieldName))
            }
            else {
                throw new FieldNotFoundException(s"Field name $fieldName is not contained its DataTable")
            }
        }
    }

     def set(fieldName: String, cell: DataCell): Unit = {
        if (table != null) {
            if (!table.contains(fieldName)) {
                table.addField(fieldName, cell.dataType)
            }
        }
        set(fieldName, cell.value, cell.dataType)
    }

    def set(fieldName: String, value: Any, dataType: DataType): Unit = {

        val name = fieldName.toLowerCase()

        if (!fields.contains(name)) {
            fields += name -> dataType
        }

        this.columns += name -> (dataType match {
                                        case DataType.DATETIME => value.toDateTime
                                        case DataType.JSON => value.toJson
                                        case _ => value
                                     })
    }

    def remove(fieldName: String): Unit = {
        val name = fieldName.toLowerCase()
        this.fields.remove(name)
        this.columns.remove(name)
    }
    
    def updateFieldName(fieldName: String, newFieldName: String): Unit = {
        this.set(newFieldName, this.columns.get(fieldName.toLowerCase()))
        this.remove(fieldName)
    }
    
    def getDataType(fieldName: String): Option[DataType] = {
        val name = fieldName.toLowerCase()
        if (this.fields.contains(name)) {
            Some(this.fields(name))
        }
        else {
            None
        }
    }
    
    def foreach(callback: (String, Any) => Unit): Unit = {
        for ((k, v) <- this.columns) {
            callback(k, v)
        }
    }
    
    def get(fieldName: String): Option[Any] = {
        val name = fieldName.toLowerCase()
        if (this.columns.contains(name)) {
            this.columns.get(name)
        }
        else {
            None
        }
    }

    //by index
    def get(index: Int): Option[Any] = {
        if (index < this.columns.size) {
            Some(this.columns.take(index + 1).last._2)
        }
        else {
            None
        }
    }

    def getCell(fieldName: String): DataCell = {
        val name = fieldName.toLowerCase()
        if (this.contains(name)) {
            DataCell(this.columns(name), this.fields(name))
        }
        else {
            DataCell.NOT_FOUND
        }
    }

    def getCell(index: Int): DataCell = {
        if (index < this.columns.size) {
            DataCell(this.columns.take(index + 1).last._2, this.fields.take(index + 1).last._2)
        }
        else {
            DataCell.NOT_FOUND
        }
    }

    def getJson(fieldName: String): Json = {
        val name = fieldName.toLowerCase()
        if (this.contains(name)) {
            this.columns(name).asInstanceOf[Json]
        }
        else {
            Json()
        }
    }

    def getString(fieldName: String): String = getString(fieldName, "")
    def getString(fieldName: String, defaultValue: String): String = getStringOption(fieldName).getOrElse(defaultValue)
    def getStringOption(fieldName: String): Option[String] = {
        get(fieldName) match {
            case Some(value) => if (value != null) Some(value.toString) else Some(null)
            case None => None
        }
    }

    def getInt(fieldName: String): Int = getInt(fieldName, 0)
    def getInt(fieldName: String, defaultValue: Int): Int = getIntOption(fieldName).getOrElse(defaultValue)
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

    def getLong(fieldName: String): Long = getLong(fieldName, 0L)
    def getLong(fieldName: String, defaultValue: Long): Long = getLongOption(fieldName).getOrElse(defaultValue)
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

    def getFloat(fieldName: String): Float = getFloat(fieldName, 0F)
    def getFloat(fieldName: String, defaultValue: Float): Float = getFloatOption(fieldName).getOrElse(defaultValue)
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

    def getDouble(fieldName: String): Double = getDouble(fieldName, 0D)
    def getDouble(fieldName: String, defaultValue: Double): Double = getDoubleOption(fieldName).getOrElse(defaultValue)
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
    
    def getBoolean(fieldName: String): Boolean = {
        get(fieldName) match {
            case Some(value) =>
                value match {
                    case bool: Boolean => bool
                    case _ =>
                        val value = getString(fieldName, "0").toLowerCase
                        value == "yes" || value == "true" || value == "1" || value == "ok"
                }
            case None => false
        }
    }

    def getDateTime(fieldName: String): DateTime = getDateTime(fieldName, DateTime.of(1970, 1, 1))
    def getDateTime(fieldName: String, defaultValue: DateTime): DateTime = getDateTimeOption(fieldName).getOrElse(defaultValue)
    def getDateTimeOption(fieldName: String): Option[DateTime] = {
        getStringOption(fieldName) match {
            case Some(value) => Some(new DateTime(value))
            case None => None
        }
    }

    def getFields: List[String] = this.fields.keySet.toList
    def getDataTypes: List[DataType] = this.fields.values.toList
    def getValues: List[Any] = this.columns.values.toList

    def getFirstString(defaultVlaue: String = ""): String = {
        this.columns.headOption match {
            case Some(field) => field._2.toString
            case None => defaultVlaue
        }
    }

    def getFirstInt(defaultValue: Int = 0): Int = {
        this.fields.headOption match {
            case Some(field) => this.getInt(field._1)
            case None => defaultValue
        }
    }

    def getFirstLong(defaultValue: Long = 0L): Long = {
        this.fields.headOption match {
            case Some(field) => this.getLong(field._1)
            case None => defaultValue
        }
    }

    def getFirstFloat(defaultValue: Float = 0F): Float = {
        this.fields.headOption match {
            case Some(field) => this.getFloat(field._1)
            case None => defaultValue
        }
    }

    def getFirstDouble(defaultValue: Double = 0D): Double = {
        this.fields.headOption match {
            case Some(field) => this.getDouble(field._1)
            case None => defaultValue
        }
    }

    def getFirstBoolean(defaultValue: Boolean = false): Boolean = {
        this.fields.headOption match {
            case Some(field) => this.getBoolean(field._1)
            case None => defaultValue
        }
    }


    
    def contains(fieldName: String): Boolean = this.columns.contains(fieldName.toLowerCase)
    def contains(fieldName: String, value: Any): Boolean = {
        val name = fieldName.toLowerCase()
        this.columns.contains(name) && this.getString(name) == value.toString
    }
    def size: Int = this.columns.size
    def isEmpty: Boolean = this.fields.isEmpty
    def nonEmpty: Boolean = this.fields.nonEmpty

    def combine(otherRow: DataRow): DataRow = {
        for ((field, value) <- otherRow.columns) {
            this.set(field, value)
        }
        this
    }

    def join(delimiter: String): String = {
        val values = new mutable.StringBuilder()
        for (field <- getFields) {
            if (values.nonEmpty) {
                values.append(", ")
            }
            values.append(this.getString(field, "null"))
        }
        values.toString()
    }

    def toJavaMap: java.util.Map[String, Any] = {
        columns.asJava
    }

    def toJavaList: java.util.List[Any] = {
        columns.values.toList.asJava
    }

    def toTable(keyName: String = "key", valueName: String = "value"): DataTable = {
        columns.toMap.toTable(keyName, valueName)
    }

    override def toString: String = {
        new ObjectMapper().writeValueAsString(toJavaMap)
    }
    
    override def equals(obj: scala.Any): Boolean = {
        this.columns == obj.asInstanceOf[DataRow].columns
    }

    override def hashCode(): Int = {
        Objects.hash(columns, fields)
    }
    
    def clear(): Unit = {
        this.columns.clear()
        this.fields.clear()
    }
}