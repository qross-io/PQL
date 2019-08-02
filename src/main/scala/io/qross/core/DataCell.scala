package io.qross.core

import DataType.DataType
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.sql.SQLExecuteException
import io.qross.time.DateTime

import scala.util.{Failure, Success, Try}

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
    def invalid: Boolean = dataType == DataType.EXCEPTION && value == "NOT_FOUND"
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

    def getString(quote: String = ""): String = {
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
    def asInteger(defaultValue: Any): Long = this.value.toInteger(defaultValue)
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
    def toJson: DataCell = {
        if (!this.isJson) {
            DataCell(this.asJson, DataType.JSON)
        }
        else {
            this
        }
    }

    def isTable: Boolean = this.dataType == DataType.TABLE
    def asTable: DataTable = this.value.toTable
    def toTable: DataCell = {
        if (!this.isTable) {
            DataCell(this.asTable, DataType.TABLE)
        }
        else {
            this
        }
    }

    def isRow: Boolean = this.dataType == DataType.ROW
    def asRow: DataRow = this.value.toRow
    def toRow: DataCell = {
        if (!this.isRow) {
            DataCell(this.asRow, DataType.ROW)
        }
        else {
            this
        }
    }

    def isJavaList: Boolean = this.dataType == DataType.LIST || this.dataType == DataType.ARRAY
    def asScalaList: List[Any] = this.value.toScalaList
    def asJavaList: java.util.List[Any] = this.value.toJavaList
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
