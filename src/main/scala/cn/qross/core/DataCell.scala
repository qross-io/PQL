package cn.qross.core

import java.util

import cn.qross.exception._
import cn.qross.ext.TypeExt._
import cn.qross.net.Json
import cn.qross.time.DateTime

import scala.collection.JavaConverters._
import scala.util.matching.Regex

object DataCell {
    val NULL: DataCell = DataCell(null, DataType.NULL) //一个DataCell的默认值, 类型未定义, 值未赋值
    val UNDEFINED: DataCell = DataCell("UNDEFINED", DataType.EXCEPTION) //表示未按预期找到想要的结果，比如变量名未找到，属性值未找到
    val ERROR: DataCell = DataCell("ERROR", DataType.EXCEPTION) //计算值时发生错误, 不支持的数据类型也在其中
    val EMPTY: DataCell = DataCell("EMPTY", DataType.NULL) //表示一种空状态, 比如列表为空, 字符串为空等
}

case class DataCell(var value: Any, var dataType: DataType= DataType.NULL) {

    def this(value: Any) {
        this(value, DataType.NULL)
    }

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
            case DataType.ARRAY | DataType.LIST => asJavaList.isEmpty
            case DataType.ROW => asRow.isEmpty
            case DataType.TABLE => asTable.isEmpty
            case DataType.HASHSET => asHashSet.isEmpty
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
                Json.serialize(value.asInstanceOf[java.util.List[Object]])
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
            case DataType.ROW => this.toRow
            case DataType.ARRAY | DataType.LIST => this.toJavaList
            case DataType.HASHSET => this.toHashSet
            case DataType.JSON => this.toJson
            case _ => throw new ConvertFailureException("Unsupported conversion format: " + dataType)
        }
    }

    def isText: Boolean = this.dataType == DataType.TEXT
    def asText: String = this.value.toText
    def asText(defaultValue: Any): String = {
        if (valid && this.value != null) {
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
                case DataType.ROW => this.value.asInstanceOf[DataRow].asTable
                    //this.value.asInstanceOf[DataRow].turnToTable("key", "value")
                case DataType.ARRAY | DataType.LIST =>
                    val table = new DataTable()
                    this.value.asInstanceOf[java.util.List[Any]].forEach(item => {
                        val row = new DataRow()
                        row.set("item", DataCell(item))
                        table += row
                    })
                    table
                case DataType.HASHSET =>
                    val table = new DataTable()
                    this.value.asInstanceOf[java.util.Set[Any]].forEach(item => {
                        val row = new DataRow()
                        row.set("item", DataCell(item))
                        table += row
                    })
                    table
                case _ => new DataTable(new DataRow("value" -> this.value))
            }
        }
        else {
            new DataTable()
        }
    }
    def asTable(fields: String*): DataTable = {
        if (valid) {
            this.dataType match {
                case DataType.TABLE => this.value.asInstanceOf[DataTable]
                case DataType.ROW => this.value.asInstanceOf[DataRow].asTable
                    //this.value.asInstanceOf[DataRow].turnToTable(if (fields.nonEmpty) fields.head else "key", if (fields.length > 1) fields(1) else "value")
                case DataType.ARRAY | DataType.LIST =>
                    val table = new DataTable()
                    val field = if (fields.nonEmpty) fields.head else "item"
                    this.value.asInstanceOf[java.util.List[Any]].forEach(item => {
                        val row = new DataRow()
                        row.set(field, DataCell(item))
                        table += row
                    })
                    table
                case DataType.HASHSET =>
                    val table = new DataTable()
                    val field = if (fields.nonEmpty) fields.head else "item"
                    this.value.asInstanceOf[java.util.Set[Any]].forEach(item => {
                        val row = new DataRow()
                        row.set(field, DataCell(item))
                        table += row
                    })
                    table
                case _ => new DataTable(new DataRow((if (fields.nonEmpty) fields.head else "value") -> this.value))
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
                case DataType.ROW => this.value.asInstanceOf[DataRow]
                case DataType.ARRAY | DataType.LIST =>
                    val list = this.value.asInstanceOf[java.util.List[Any]]
                    val row = new DataRow()
                    for (i <- 0 until list.size()) {
                        row.set("item_" + i, list.get(i))
                    }
                    row
                case DataType.HASHSET =>
                    val list = this.value.asInstanceOf[java.util.Set[Any]].asScala
                    val row = new DataRow()
                    for (i <- 0 until list.size) {
                        row.set("item_" + i, list(i))
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
                case DataType.ROW => this.value.asInstanceOf[DataRow].getValues[T]
                case DataType.ARRAY | DataType.LIST => this.value.asInstanceOf[java.util.List[T]].asScala.toList
                case DataType.HASHSET => this.value.asInstanceOf[java.util.Set[T]].asScala.toList
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
                case DataType.ROW => this.value.asInstanceOf[DataRow].toJavaList
                case DataType.ARRAY | DataType.LIST => this.value.asInstanceOf[java.util.List[Any]]
                case DataType.HASHSET => new util.ArrayList[Any](this.value.asInstanceOf[java.util.Set[Any]])
                case DataType.TEXT =>
                    val list = new util.ArrayList[Any]()
                    this.value.asInstanceOf[String].split("").foreach(list.add)
                    list
                case _ => List[Any](this.value).asJava
            }
        }
        else {
            new java.util.ArrayList[Any]()
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

    def isHashSet: Boolean = this.dataType == DataType.HASHSET
    def asHashSet: java.util.Set[Any] = {
        if (valid) {
            this.dataType match {
                case DataType.TABLE => this.value.asInstanceOf[DataTable].toHashSet
                case DataType.ROW => this.value.asInstanceOf[DataRow].toHashSet
                case DataType.ARRAY | DataType.LIST => new java.util.HashSet[Any](this.value.asInstanceOf[java.util.List[Any]])
                case DataType.HASHSET => this.value.asInstanceOf[java.util.Set[Any]]
                case DataType.TEXT =>
                    val set = new java.util.HashSet[Any]()
                    this.value.asInstanceOf[String].split("").foreach(set.add)
                    set
                case _ => Set[Any](this.value).asJava
            }
        }
        else {
            new java.util.HashSet[Any]()
        }
    }
    def toHashSet: DataCell = {
        if (!this.isHashSet) {
            DataCell(this.asHashSet, DataType.HASHSET)
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

    def getDataByProperty(attr: String): DataCell = {

        this.dataType match {
            case DataType.ROW => this.asRow.getCell(attr)
            case DataType.TABLE =>
                val table = this.asTable
                if (table.contains(attr)) {
                    this.asTable.getColumn(attr).toJavaList.toDataCell(DataType.ARRAY)
                }
                else if (attr.equalsIgnoreCase("first")) {
                    table.firstRow match {
                        case Some(row) => DataCell(row, DataType.ROW)
                        case None => throw new OutOfIndexBoundaryException("Table doesn't contains any rows.")
                    }
                }
                else if (attr.equalsIgnoreCase("last")) {
                    table.lastRow match {
                        case Some(row) => DataCell(row, DataType.ROW)
                        case None => throw new OutOfIndexBoundaryException("Table doesn't contains any rows.")
                    }
                }
                else {
                    throw new TableColumnNotFoundException(s"Table doesn't contains column '$attr'.")
                }
            case DataType.ARRAY | DataType.LIST =>
                val list = this.asJavaList
                if (!list.isEmpty) {
                    if (attr.equalsIgnoreCase("first")) {
                        DataCell(list.get(0))
                    }
                    else if (attr.equalsIgnoreCase("last")) {
                        DataCell(list.get(list.size() - 1))
                    }
                    else {
                        throw new IncorrectPropertyNameException(s"Array/List doesn't contains property $attr.")
                    }
                }
                else {
                    throw new OutOfIndexBoundaryException("Array/List is empty.")
                }
            case DataType.HASHSET =>
                val list = this.asHashSet.asScala
                if (list.nonEmpty) {
                    if (attr.equalsIgnoreCase("first")) {
                        DataCell(list.head)
                    }
                    else if (attr.equalsIgnoreCase("last")) {
                        DataCell(list.last)
                    }
                    else {
                        throw new IncorrectPropertyNameException(s"HashSet doesn't contains property $attr.")
                    }
                }
                else {
                    throw new OutOfIndexBoundaryException("HashSet is empty.")
                }
            case _ =>
                if (this.isExtensionType) {
                    try {
                        Class.forName(this.dataType.typeName)
                            .getDeclaredMethod("getCell", classOf[String]) // Class.forName("java.lang.String")
                            .invoke(this.value, attr)
                            .asInstanceOf[DataCell]
                    }
                    catch {
                        case _: Exception => throw new ClassMethodNotFoundException("Class " + this.dataType.typeName + " must contains method getCell(String).")
                    }
                }
                else {
                    throw new UnsupportedDataTypeException("Only collection data type supports method 'getDataByProperty'.")
                }
        }
    }

    def getDataByIndex(expression: String): DataCell = {

        val value = expression.eval()
        this.dataType match {
            case DataType.ARRAY | DataType.LIST =>
                val list = this.asJavaList
                if (!list.isEmpty) {
                    if (value.isInteger || value.isDecimal) {
                        val index = value.asInteger.toInt //索引从0开始
                        if (index < list.size()) {
                            DataCell(list.get(index))
                        }
                        else {
                            throw new OutOfIndexBoundaryException(s"Index $index is greater than Array/List size.")
                        }
                    }
                    else if (value.isText) {
                        val attr = value.asText
                        if (attr.equalsIgnoreCase("first")) {
                            DataCell(list.get(0))
                        }
                        else if (attr.equalsIgnoreCase("last")) {
                            DataCell(list.get(list.size() - 1))
                        }
                        else {
                            throw new IncorrectPropertyNameException(s"Array/List doesn't contains property $attr.")
                        }
                    }
                    else {
                        throw new IncorrectIndexDataTypeException("Only supports integer index in Array/List.")
                    }
                }
                else {
                    throw new OutOfIndexBoundaryException("Array/List is empty.")
                }
            case DataType.ROW =>
                val row = this.asRow
                if (value.isInteger) {
                    val index = value.asInteger.toInt
                    if (index < row.size) {
                        row.getCell(index)
                    }
                    else {
                        throw new OutOfIndexBoundaryException(s"Index ${index + 1} is greater than Row size.")
                    }
                }
                else if (value.isText) {
                    val attr = value.asText
                    if (row.contains(attr)) {
                        row.getCell(attr)
                    }
                    else {
                        throw new IncorrectPropertyNameException(s"Row doesn't contains property $attr.")
                    }
                }
                else {
                    throw new IncorrectIndexDataTypeException("Only supports string and integer index in Row.")
                }
            case DataType.TABLE =>
                val table = this.asTable
                if (value.isInteger) {
                    val index = value.asInteger.toInt //索引从1开始
                    if (table.nonEmpty) {
                        table.getRow(index) match {
                            case Some(row) => DataCell(row, DataType.ROW)
                            case None => throw new OutOfIndexBoundaryException(s"Index $index is greater than Table size.")
                        }
                    }
                    else {
                        throw new OutOfIndexBoundaryException("Table is empty.")
                    }
                }
                else if (value.isText) {
                    val attr = value.asText
                    if (table.contains(attr)) {
                        DataCell(table.getColumn(attr).toJavaList, DataType.ARRAY)
                    }
                    else if (attr.equalsIgnoreCase("first")) {
                        table.firstRow match {
                            case Some(row) => DataCell(row, DataType.ROW)
                            case None => throw new OutOfIndexBoundaryException("Table is empty.")
                        }
                    }
                    else if (attr.equalsIgnoreCase("last")) {
                        table.lastRow match {
                            case Some(row) => DataCell(row, DataType.ROW)
                            case None => throw new OutOfIndexBoundaryException("Table is empty.")
                        }
                    }
                    else {
                        throw new IncorrectPropertyNameException(s"Row doesn't contains column $attr.")
                    }
                }
                else {
                    throw new IncorrectIndexDataTypeException("Only supports string and integer index in Table.")
                }
            case DataType.HASHSET =>
                val list = this.asHashSet.asScala
                if (list.nonEmpty) {
                    if (value.isInteger || value.isDecimal) {
                        val index = value.asInteger.toInt //索引从0开始
                        if (index < list.size) {
                            DataCell(list(index))
                        }
                        else {
                            throw new OutOfIndexBoundaryException(s"Index $index is greater than HashSet size.")
                        }
                    }
                    else if (value.isText) {
                        val attr = value.asText
                        if (attr.equalsIgnoreCase("first")) {
                            DataCell(list.head)
                        }
                        else if (attr.equalsIgnoreCase("last")) {
                            DataCell(list.last)
                        }
                        else {
                            throw new IncorrectPropertyNameException(s"HashSet doesn't contains property $attr.")
                        }
                    }
                    else {
                        throw new IncorrectIndexDataTypeException("Only supports integer index in HashSet.")
                    }
                }
                else {
                    throw new OutOfIndexBoundaryException("HashSet is empty.")
                }
            case _ =>
                if (this.isExtensionType) {
                    if (value.isInteger) {
                        val index = value.asInteger.toInt //索引从1开始
                        try {
                            Class.forName(this.dataType.typeName)
                                .getDeclaredMethod("getCell", classOf[Int])  //Class.forName("java.lang.Integer")
                                .invoke(this.value, Integer.valueOf(index))
                                .asInstanceOf[DataCell]
                        }
                        catch {
                            case _: Exception => throw new ClassMethodNotFoundException("Class " + this.dataType.typeName + " must contains method getCell(Integer).")
                        }
                    }
                    else if (value.isText) {
                        val attr = value.asText
                        try {
                            Class.forName(this.dataType.typeName)
                                .getDeclaredMethod("getCell", classOf[String]) //Class.forName("java.lang.String")
                                .invoke(this.value, attr)
                                .asInstanceOf[DataCell]
                        }
                        catch {
                            case _: Exception => throw new ClassMethodNotFoundException("Class " + this.dataType.typeName + " must contains method getCell(String).")
                        }
                    }
                    else {
                        throw new IncorrectIndexDataTypeException("Only supports string and integer index in extension Class.")
                    }

                }
                else {
                    throw new UnsupportedDataTypeException("Only collection data type supports method 'getDataByIndex'.")
                }
        }
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

    def +(implicit other: DataCell): DataCell = {
        if (this.isInteger && other.isInteger) {
            DataCell(this.asInteger + other.asInteger)
        }
        else {
            DataCell(this.asDecimal + other.asDecimal)
        }
    }

    override def toString: String = {
        if (value == null) {
            null
        }
        else if (dataType == DataType.ARRAY || dataType == DataType.LIST) {
            Json.serialize(value.asInstanceOf[AnyRef])
        }
        else if (DataType.isInternalType(dataType)) {
            value.toString
        }
        else {
            Json.serialize(value.asInstanceOf[AnyRef])
        }
    }
}