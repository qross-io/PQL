package io.qross.core

import java.util

import io.qross.core.DataType.DataType
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.time.DateTime

import scala.collection.JavaConverters._

object DataCell {
    val NOT_FOUND: DataCell = DataCell("NOT_FOUND", DataType.EXCEPTION)
    val NULL: DataCell = DataCell(null, DataType.NULL)
    val EMPTY: DataCell = DataCell("", DataType.TEXT)
}

case class DataCell(var value: Any, var dataType: DataType = DataType.NULL) {

    if (value != null && dataType == DataType.NULL) {
        dataType = DataType.ofValue(value)
    }

//    def isNull: Boolean = value == null && dataType == DataType.NULL
//    def isNotNull: Boolean = value != null || dataType != DataType.NULL
    def isNull: Boolean = value == null
    def isNotNull: Boolean = value == null
    def isEmpty: Boolean = value == ""
    def isNotEmpty: Boolean = value != ""
    def notFound: Boolean = dataType == DataType.EXCEPTION && value == "NOT_FOUND"
    def found: Boolean = !notFound
    def invalid: Boolean = dataType == DataType.EXCEPTION
    def valid: Boolean = !invalid

    def data: Option[Any] = Option(value)

    def ifNotNull(handler: DataCell => Unit): DataCell = {
        if (isNotNull) {
            handler(this)
        }
        this
    }

    def ifNull(handler: () => Unit): DataCell = {
        if (isNull) {
            handler()
        }
        this
    }

    def ifFound(handler: DataCell => Unit): DataCell = {
        if (found) {
            handler(this)
        }
        this
    }

    def ifNotFound(handler: () => Unit): DataCell = {
        if (notFound) {
            handler()
        }
        this
    }

    def ifValid(handler: DataCell => Unit): DataCell = {
        if (valid) {
            handler(this)
        }
        this
    }

    def ifInvalid(handler: () => Unit): DataCell = {
        if (invalid) {
            handler()
        }
        this
    }

    def orElse(data: DataCell): DataCell = {
        if (valid) {
            this
        }
        else {
            data
        }
    }

    def orElse(value: Any, dataType: DataType = DataType.NULL): DataCell = {
        if (valid) {
            this
        }
        else {
            DataCell(value,
                if (value != null && dataType == DataType.NULL) {
                    DataType.ofValue(value)
                }
                else {
                    dataType
                })
        }
    }

    def mkString(quote: String = ""): String = {
        if (value != null) {
            value.toString.userQuotesIf(quote, dataType == DataType.TEXT || dataType == DataType.DATETIME)
        }
        else {
            "null"
        }
    }

    def update(value: Any, dataType: DataType = DataType.NULL): DataCell = {
        this.value = value
        //更新类型
        if (this.dataType != dataType) {
            this.dataType = dataType
        }
        //自动识别类型
        if (value != null && dataType == DataType.NULL) {
            this.dataType = DataType.ofValue(value)
        }

        this
    }

    def replace(cell: DataCell): DataCell = {
        this.value = cell.value
        this.dataType = cell.dataType

        this
    }

    def to(dataType: DataType): DataCell = {
        dataType match {
            case DataType.AUTO => this
            case DataType.TEXT => this.toText
            case DataType.INTEGER => this.toInteger
            case DataType.DECIMAL => this.toDecimal
            case DataType.BOOLEAN => this.toBoolean
            case DataType.DATETIME => this.toDateTime
            case DataType.JSON => this.toJson
            case DataType.TABLE => this.toTable
            case _ => throw new ConvertFailureException("Unsupported conversion format: " + dataType)
        }
    }

    def isText: Boolean = this.dataType == DataType.TEXT
    def asText: String = this.value.toText
    def asText(defaultValue: Any): String = {
        if (valid) {
            this.valid.toText
        }
        else {
            defaultValue.toText
        }
    }
    def toText: DataCell = {
        if (!this.isText) {
            DataCell(this.asText, DataType.TEXT)
        }
        else {
            this
        }
    }

    def isInteger: Boolean = this.dataType == DataType.INTEGER
    def asInteger: Long = this.value.toInteger
    def asInteger(defaultValue: Any): Long = {
        if (valid) {
            this.value.toInteger(defaultValue)
        }
        else {
            defaultValue.toInteger
        }
    }
    def toInteger: DataCell = {
        if (!this.isInteger) {
            DataCell(this.asInteger, DataType.INTEGER)
        }
        else {
            this
        }
    }

    def isDecimal: Boolean = this.dataType == DataType.DECIMAL
    def asDecimal: Double = this.value.toDecimal
    def asDecimal(defaultValue: Any): Double = {
        if (valid) {
            this.value.toDecimal(defaultValue)
        }
        else {
            defaultValue.toDecimal
        }
    }
    def toDecimal: DataCell = {
        if (!this.isDecimal) {
            DataCell(this.asDecimal, DataType.DECIMAL)
        }
        else {
            this
        }
    }

    def isBoolean: Boolean = this.dataType == DataType.BOOLEAN
    def asBoolean: Boolean = this.value.toBoolean
    def asBoolean(defaultValue: Any): Boolean = {
        if (valid) {
            this.value.toBoolean(defaultValue)
        }
        else {
            defaultValue.toBoolean
        }
    }
    def toBoolean: DataCell = {
        if (!this.isBoolean) {
            DataCell(this.asBoolean, DataType.BOOLEAN)
        }
        else {
            this
        }
    }

    def isDateTime: Boolean = this.dataType == DataType.DATETIME
    def asDateTime: DateTime = this.value.toDateTime
    def asDateTime(defaultValue: Any): DateTime = {
        if (valid) {
            this.value.toDateTime(defaultValue)
        }
        else {
            defaultValue.toDateTime
        }
    }
    def toDateTime: DataCell = {
        if (!this.isDateTime) {
            DataCell(this.asDateTime, DataType.DATETIME)
        }
        else {
            this
        }
    }

    def isJson: Boolean = this.dataType == DataType.JSON
    def asJson: Json = this.value.toJson
    def asJson(defaultValue: Any): Json = {
        if (valid) {
            this.value.toJson(defaultValue)
        }
        else {
            defaultValue.toJson
        }
    }
    def toJson: DataCell = {
        if (!this.isJson) {
            DataCell(this.asJson, DataType.JSON)
        }
        else {
            this
        }
    }

    def isTable: Boolean = this.dataType == DataType.TABLE
    def asTable: DataTable = {
        if (valid) {
            this.dataType match {
                case DataType.TABLE => this.value.asInstanceOf[DataTable]
                case DataType.ROW | DataType.MAP | DataType.OBJECT => this.value.asInstanceOf[DataRow].toTable("field")
                case DataType.ARRAY | DataType.LIST => this.value.asInstanceOf[java.util.List[Any]].asScala.toList.toTable()
                case _ => new DataTable(new DataRow("value" -> this.value))
            }
        }
        else {
            new DataTable()
        }
    }
    def toTable: DataCell = {
        if (!this.isTable) {
            DataCell(this.asTable, DataType.TABLE)
        }
        else {
            this
        }
    }

    def isRow: Boolean = this.dataType == DataType.ROW
    def asRow: DataRow = {
        if (valid) {
            this.dataType match {
                case DataType.TABLE => this.value.asInstanceOf[DataTable].firstRow.getOrElse(new DataRow())
                case DataType.ROW | DataType.MAP | DataType.OBJECT => this.value.asInstanceOf[DataRow]
                case DataType.ARRAY | DataType.LIST =>
                    val list = this.value.asInstanceOf[java.util.List[Any]]
                    val row = new DataRow()
                    for (i <- 0 until list.size()) {
                        row.set("item_" + i, list.get(i))
                    }
                    row
                case _ => new DataRow("value" -> this.value)
            }
        }
        else {
            new DataRow()
        }
    }
    def toRow: DataCell = {
        if (!this.isRow) {
            DataCell(this.asRow, DataType.ROW)
        }
        else {
            this
        }
    }

    def isJavaList: Boolean = this.dataType == DataType.LIST || this.dataType == DataType.ARRAY
    def asList: List[Any] = {
        if (valid) {
            this.dataType match {
                case DataType.TABLE => this.value.asInstanceOf[DataTable].toList
                case DataType.ROW | DataType.MAP | DataType.OBJECT => this.value.asInstanceOf[DataRow].getValues
                case DataType.ARRAY | DataType.LIST => this.value.asInstanceOf[java.util.List[Any]].asScala.toList
                case _ => List[Any](this.value)
            }
        }
        else {
            List[Any]()
        }
    }
    def asJavaList: java.util.List[Any] = {
        if (valid) {
            this.dataType match {
                case DataType.TABLE => this.value.asInstanceOf[DataTable].toJavaList
                case DataType.ROW | DataType.MAP | DataType.OBJECT => this.value.asInstanceOf[DataRow].toJavaList
                case DataType.ARRAY | DataType.LIST => this.value.asInstanceOf[java.util.List[Any]]
                case _ => List[Any](this.value).asJava
            }
        }
        else {
            new util.ArrayList[Any]()
        }
    }
    def toJavaList: DataCell = {
        if (!this.isJavaList) {
            DataCell(this.asJavaList, DataType.ARRAY)
        }
        else {
            this
        }
    }

    override def toString: String = {
        if (value == null) {
            null
        }
        else {
            value.toString
        }
    }
}
