package io.qross.core

import java.util

import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.time.DateTime

import scala.collection.JavaConverters._
import scala.util.matching.Regex

object DataCell {
    val NULL: DataCell = DataCell(null, DataType.NULL) //一个DataCell的默认值, 类型未定义, 值未赋值
    val UNDEFINED: DataCell = DataCell("UNDEFINED", DataType.EXCEPTION) //表示未按预期找到想要的结果，比如变量名未找到，属性值未找到
    val ERROR: DataCell = DataCell("ERROR", DataType.EXCEPTION) //计算值时发生错误, 不支持的数据类型也在其中
    val EMPTY: DataCell = DataCell("EMPTY", DataType.NULL) //表示一种空状态, 比如列表为空, 字符串为空等
}

case class DataCell(var value: Any, var dataType: DataType = DataType.NULL) {

    if (value != null && dataType == DataType.NULL) {
        dataType = DataType.ofValue(value)
    }
    //DataCell存储的基本数据类型必须正确
    dataType match {
        case DataType.TEXT => if (!value.isInstanceOf[String] && value != null) value = value.toString
        case DataType.INTEGER => if (!(value.isInstanceOf[Int] || value.isInstanceOf[Long]) && value != null) value = value.toInteger
        case DataType.DECIMAL => if (!(value.isInstanceOf[Int] || value.isInstanceOf[Long] || value.isInstanceOf[Float] || value.isInstanceOf[Double]) && value != null) value = value.toDecimal
        case DataType.BOOLEAN => if (!value.isInstanceOf[Boolean] && value != null) value = value.toBoolean
        case DataType.DATETIME => if (!value.isInstanceOf[DateTime] && value != null) value = value.toDateTime
        case _ =>
    }

    def isNull: Boolean = value == null && dataType == DataType.NULL
    def nonNull: Boolean = !isNull
    def isEmpty: Boolean = {
        dataType match {
            case DataType.TEXT => asText == ""
            case DataType.ARRAY | DataType.LIST => asJavaList.size() == 0
            case DataType.ROW | DataType.MAP | DataType.OBJECT => asRow.isEmpty
            case DataType.TABLE => asTable.isEmpty
            case DataType.NULL | DataType.EXCEPTION => true
            case _ => value == null
        }
    }
    def nonEmpty: Boolean = !isEmpty
    def defined: Boolean = !undefined
    def undefined: Boolean = dataType == DataType.EXCEPTION && value == "UNDEFINED"
    def isError: Boolean = dataType == DataType.EXCEPTION && value == "ERROR"
    def nonError: Boolean = !isError
    def isExceptional: Boolean = dataType == DataType.EXCEPTION
    def nonExceptional: Boolean = dataType != DataType.EXCEPTION
    def invalid: Boolean = dataType == DataType.EXCEPTION || dataType == DataType.NULL
    def valid: Boolean = !invalid

    def data: Option[Any] = Option(value)

    def ifNull(handler: () => Unit): DataCell = {
        if (isNull) {
            handler()
        }
        this
    }

    def ifNotNull(handler: DataCell => Unit): DataCell = {
        if (nonNull) {
            handler(this)
        }
        this
    }

    def ifEmpty(handler: () => Unit): DataCell = {
        if (isEmpty) {
            handler()
        }
        this
    }

    def ifNotEmpty(handler: DataCell => Unit): DataCell = {
        if (nonEmpty) {
            handler(this)
        }
        this
    }

    def ifFound(handler: DataCell => Unit): DataCell = {
        if (defined) {
            handler(this)
        }
        this
    }

    def ifNotFound(handler: () => Unit): DataCell = {
        if (undefined) {
            handler()
        }
        this
    }

    def ifErrorOccurred(handler: DataCell => Unit): DataCell = {
        if (isError) {
            handler(this)
        }
        this
    }

    def ifErrorNotOccurred(handler: () => Unit): DataCell = {
        if (nonError) {
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
            if (this.dataType != DataType.ARRAY) {
                value.toString.userQuotesIf(quote, dataType == DataType.TEXT || dataType == DataType.DATETIME)
            }
            else {
                Json.serialize(value.asInstanceOf[java.util.ArrayList[Object]])
            }
        }
        else {
            "null"
        }
    }

    def update(value: Any): DataCell = {
        this.value = value
        this
    }

    def update(value: Any, dataType: DataType): DataCell = {
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
            case DataType.TEXT => this.toText
            case DataType.INTEGER => this.toInteger
            case DataType.DECIMAL => this.toDecimal
            case DataType.BOOLEAN => this.toBoolean
            case DataType.DATETIME => this.toDateTime
            case DataType.TABLE => this.toTable
            case DataType.ROW | DataType.OBJECT | DataType.MAP => this.toRow
            case DataType.ARRAY | DataType.LIST => this.toJavaList
            case DataType.JSON => this.toJson
            case _ => throw new ConvertFailureException("Unsupported conversion format: " + dataType)
        }
    }

    def isText: Boolean = this.dataType == DataType.TEXT
    def asText: String = this.value.toText
    def asText(defaultValue: Any): String = {
        if (valid) {
            this.value.toText
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

    def isRegex: Boolean = this.dataType == DataType.REGEX
    def asRegex: Regex = this.value.toRegex
    def toRegex: DataCell = {
        if (!this.isRegex) {
            DataCell(this.asRegex, DataType.REGEX)
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
    def asDateTime(format: String): DateTime = this.value.toDateTime(format)
    def asDateTimeOrElse(defaultValue: DateTime): DateTime = this.value.toDateTimeOrElse(defaultValue)
    def asDateTimeOrElse(format: String, defaultValue: DateTime): DateTime = this.value.toDateTimeOrElse(format, defaultValue)
    def toDateTime: DataCell = {
        if (!this.isDateTime) {
            DataCell(this.asDateTime, DataType.DATETIME)
        }
        else {
            this
        }
    }

    def isJson: Boolean = this.dataType == DataType.JSON
    def asJson: Json = value.toJson
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
                case DataType.ROW | DataType.MAP | DataType.OBJECT => this.value.asInstanceOf[DataRow].toTable
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
    def asList[T]: List[T] = {
        if (valid) {
            this.dataType match {
                case DataType.TABLE => this.value.asInstanceOf[DataTable].toList[T]
                case DataType.ROW | DataType.MAP | DataType.OBJECT => this.value.asInstanceOf[DataRow].getValues[T]
                case DataType.ARRAY | DataType.LIST => this.value.asInstanceOf[java.util.List[T]].asScala.toList
                case DataType.TEXT => this.value.asInstanceOf[String].split("").asInstanceOf[List[T]]
                case _ => List[T](this.value.asInstanceOf[T])
            }
        }
        else {
            List[T]()
        }
    }
    def asJavaList: java.util.List[Any] = {
        if (valid) {
            this.dataType match {
                case DataType.TABLE => this.value.asInstanceOf[DataTable].toJavaList
                case DataType.ROW | DataType.MAP | DataType.OBJECT => this.value.asInstanceOf[DataRow].toJavaList
                case DataType.ARRAY | DataType.LIST => this.value.asInstanceOf[java.util.List[Any]]
                case DataType.TEXT =>
                    val list = new util.ArrayList[Any]()
                    this.value.asInstanceOf[String].split("").foreach(list.add)
                    list
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

    def is(dataType: String): Boolean = {
        this.dataType.typeName == dataType || this.dataType.className == dataType || this.dataType.originalName == dataType
    }

    def isExtensionType: Boolean = {
        this.dataType.typeName.contains(".")
    }

    def asClass: Unit = {
        Class.forName(this.dataType.typeName).getClass.getDeclaredMethod("getCell")
        //this.value
    }

    def toOption[T]: Option[T] = {
        if (this.valid) {
            Some(this.value.asInstanceOf[T])
        }
        else {
            None
        }
    }

    def >(implicit other: DataCell): Boolean = {
        if (this.isInteger || this.isDecimal || other.isInteger || other.isDecimal) {
            this.asDecimal > other.asDecimal
        }
        else if (this.isDateTime || other.isDateTime) {
            this.asDateTime.after(other.asDateTime)
        }
        else {
            this.asText > other.asText
        }
    }

    def >=(implicit other: DataCell): Boolean = {
        if (this.isInteger || this.isDecimal || other.isInteger || other.isDecimal) {
            this.asDecimal >= other.asDecimal
        }
        else if (this.isDateTime || other.isDateTime) {
            this.asDateTime.afterOrEquals(other.asDateTime)
        }
        else {
            this.asText >= other.asText
        }
    }

    def <(implicit other: DataCell): Boolean = {
        if (this.isInteger || this.isDecimal || other.isInteger || other.isDecimal) {
            this.asDecimal < other.asDecimal
        }
        else if (this.isDateTime || other.isDateTime) {
            this.asDateTime.before(other.asDateTime)
        }
        else {
            this.asText < other.asText
        }
    }

    def <=(implicit other: DataCell): Boolean = {
        if (this.isInteger || this.isDecimal || other.isInteger || other.isDecimal) {
            this.asDecimal <= other.asDecimal
        }
        else if (this.isDateTime || other.isDateTime) {
            this.asDateTime.beforeOrEquals(other.asDateTime)
        }
        else {
            this.asText <= other.asText
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