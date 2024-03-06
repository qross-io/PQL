package cn.qross.fs

import java.io.{File, RandomAccessFile}

import cn.qross.core.{DataHub, DataRow, DataTable, DataType}
import cn.qross.exception.IncorrectDataSourceException
import cn.qross.ext.TypeExt._
import cn.qross.fql.{Column, SELECT}
import cn.qross.fs.Path._
import cn.qross.net.Json
import cn.qross.setting.Global

import scala.collection.mutable
import scala.util.control.Breaks._

object TextFile {

    val TXT = 1
    val CSV = 2
    val JSON = 3
    val GZ = 4

    val FILE = "file"
    val STREAM = "stream"

    val TERMINATOR: String = System.getProperty("line.separator")

    implicit class DataHub$TextFile(val dh: DataHub) {

        /*
        def useDelimiter(delimiter: String): DataHub = {
            READER.delimit(delimiter)
            this
        }

        def etl(handler: DataTable => DataTable): DataHub = {
            READER.etl(handler)
            this
        }

        /*
        openTextFile("")
            .asTable("")
            .withColumns("")
            .etl(handler)
            .page("SELECT * FROM tableName LIMIT @offset, 10000")

            .get("")
            .page("")
            .block()
        .saveAs("")
            .put("")
         */

        def readAllAsTable(fields: String*): DataHub = {
            TABLE.cut(READER.readAllAsTable(fields: _*))
            this
        }
        */

        def openTextFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.TXT))
            dh
        }

        def openTextFile(file: File): DataHub = {
            dh.FQL.create(file.getAbsolutePath, new TextFile(file, TextFile.TXT))
            dh
        }

        def openJsonFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.JSON).bracketedBy("{", "}"))
            dh
        }

        def openJsonFile(file: File): DataHub = {
            dh.FQL.create(file.getAbsolutePath, new TextFile(file, TextFile.JSON).bracketedBy("{", "}"))
            dh
        }

        def openCsvFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.CSV).delimitedBy(","))
            dh
        }

        def openCsvFile(file: File): DataHub = {
            dh.FQL.create(file.getAbsolutePath, new TextFile(file, TextFile.CSV).delimitedBy(","))
            dh
        }

        //暂时未实现
        def openGzFile(fileNameOrFullPath: String, delimiter: String = ","): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new FileReader(fileNameOrFullPath, TextFile.GZ, delimiter))
            dh
        }

        def withColumns(fields: String*): DataHub = {
            dh.FQL.getTable match {
                case file: TextFile => file.withColumns(fields: _*)
                case _ =>
            }
            dh
        }

        def withColumnsOfFirstRow(): DataHub = {
            dh.FQL.getTable match {
                case file: TextFile => file.withColumnsOfFirstRow()
                case _ =>
            }
            dh
        }

        def bracketedBy(head: String, tail: String = ""): DataHub = {
            dh.FQL.getTable match {
                case file: TextFile => file.bracketedBy(head, tail)
                case _ =>
            }
            dh
        }

        //适用于openSource中的分隔符修改
        def delimitedBy(delimiter: String): DataHub = {
            dh.FQL.getTable match {
                case file: TextFile => file.delimitedBy(delimiter)
                case _ =>
            }
            dh
        }

        def skipLines(amount: Int): DataHub = {
            dh.FQL.getTable match {
                case file: TextFile => file.skipLines(amount)
                case _ =>
            }
            dh
        }

        def cursor: Long = {
            dh.FQL.getTable match {
                case file: TextFile => file.cursor
                case _ => 0
            }
        }

        def saveAsJsonFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new FileWriter(fileNameOrFullPath, TextFile.JSON, TextFile.FILE, true))
            dh
        }

        def saveToJsonFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new FileWriter(fileNameOrFullPath, TextFile.JSON, TextFile.FILE, false))
            dh
        }

        def saveAsStreamJsonFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new FileWriter(fileNameOrFullPath, TextFile.JSON, TextFile.STREAM))
            dh
        }

        def saveAsTextFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new FileWriter(fileNameOrFullPath, TextFile.TXT, TextFile.FILE, true))
            dh
        }

        def saveToTextFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new FileWriter(fileNameOrFullPath, TextFile.TXT, TextFile.FILE, false))
            dh
        }

        def saveAsStreamFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new FileWriter(fileNameOrFullPath, TextFile.TXT, TextFile.STREAM))
            dh
        }

        def saveAsCsvFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new FileWriter(fileNameOrFullPath, TextFile.CSV, TextFile.FILE, true))
            dh
        }

        def saveToCsvFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new FileWriter(fileNameOrFullPath, TextFile.CSV, TextFile.FILE, false))
            dh
        }

        def saveAsStreamCsvFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new FileWriter(fileNameOrFullPath, TextFile.CSV, TextFile.STREAM))
            dh
        }

        //适用于saveDestination中的分隔符修改
        def delimit(delimiter: String): DataHub = {
            dh.FQL.getTable match {
                case writer: FileWriter => writer.delimit(delimiter)
                case _ =>
            }
            dh
        }

        def withHeaders(): DataHub = {
            dh.plug("WITH_HEADERS", true)
        }

        def withoutHeaders(): DataHub = {
            dh.plug("WITH_HEADERS", false)
        }

        def withHeaders(labels: Array[String]): DataHub = {
            dh.label(labels)
        }

        def withHeaders(labels: String): DataHub = {
            dh.plug("WITH_HEADERS", true)
            if (labels.bracketsWith("{", "}")) {
                dh.label(
                    Json(labels)
                        .parseRow("/")
                        .toSeq
                        .map(alias => (alias._1, alias._2.asInstanceOf[String])): _*)
            }
            else if (labels.bracketsWith("(", ")")) {
                dh.label(labels.$trim("(", ")").split(",").map(_.trim()))
            }
            else {
                dh.label(labels)
            }
        }

        def withHeaders(labels: (String, String)*): DataHub = {
            dh.plug("WITH_HEADERS", true)
            if (labels.length == 1 && labels(0)._1 == "*") {
                dh
            }
            else {
                dh.label(labels: _*)
            }
        }

        def write(): DataHub = {
            dh.FQL.getTable match {
                case writer: FileWriter =>
                    writer.writeTable(dh.getData, dh.pick[Boolean]("WITH_HEADERS").getOrElse(true))

                    if (dh.slots("ZIP")) {
                        dh.pick[Zip]("ZIP").orNull.addFile(writer.file.getAbsolutePath)
                    }

                    dh.TO_BE_CLEAR = true
                case _ => throw new IncorrectDataSourceException("Must use SAVE sentence to save file first.")
            }
            dh
        }
    }
}

class TextFile(val file: File, val format: Int) {

    private lazy val access = new RandomAccessFile(file, "r") //read

    private var header = false //首行是否为表头
    private var first = true //是否第一次读
    private var row = 0 //行号
    private var skip = 0 //如果是从头读, 略过多少行
    private var meet = 0 //满足条件的行数
    private var head = "" //row head mark
    private var tail = "" //row tail mark
    private var separator = "," //row delimiter
    private val columns = new mutable.LinkedHashMap[String, DataType]()

    private var values = "" //中间变量
    private val table = new DataTable()  //result
    private var SELECT: SELECT = _

    def this(fileNameOrPath: String, format: Int) {
        this(new File(fileNameOrPath.locate()), format)
    }

    def this(fileNameOrPath: String) {
        this(new File(fileNameOrPath.locate()), TextFile.TXT)
    }

    def this(file: File) {
        this(file, TextFile.TXT)
    }

    def withColumns(fields: String*): TextFile = {
        fields.foreach(field => {
            """\s""".r.findFirstIn(field) match {
                case Some(blank) => columns += field.takeBefore(blank).trim() -> DataType.ofTypeName(field.takeAfter(blank).trim())
                case None => columns += field -> DataType.TEXT
            }
        })
        this
    }

    def withColumnsOfFirstRow(): TextFile = {
        header = true
        skip = 1
        this
    }

    def bracketedBy(start: String, end: String = ""): TextFile = {
        head = start
        tail = end
        this
    }

    def delimitedBy(delimiter: String): TextFile = {
        separator = delimiter
        this
    }

    def skipLines(amount: Int): TextFile = {
        skip = amount
        this
    }

    def cursor: Long = access.getFilePointer

    def seek(position: Long): Unit = {
        if (position >= 0) {
            access.seek(position)
        }
        else {
            access.seek(access.length())
        }
    }

    def length: Long = access.length()

    def hasNextLine: Boolean = {
        access.getFilePointer < access.length()
    }

    def readLine(): String = {
        val line = access.readLine()
        if (line != null) {
            new String(line.getBytes("ISO-8859-1"), Global.CHARSET)
        }
        else {
            line
        }
    }

    def readLine(head: String, tail: String): String = {
        var row: String = null

        breakable {
            while (true) {
                val line = readLine()
                if (line != null) {
                    //head 为空 tail 为空，读一行是一行
                    if (head == "" && tail == "") {
                        row = line
                        break
                    }
                    //head 不为空 tail 为空
                    else if (head != "" && tail == "") {
                        if (line.startsWith(head)) {
                            //新一行
                            row = values
                            values = line
                            break
                        }
                        else {
                            values += TextFile.TERMINATOR + line
                        }
                    }
                    //head 为空 tail 不为空
                    else if (head == "" && tail != "") {
                        if (line.endsWith(tail)) {
                            if (values != "") {
                                values += TextFile.TERMINATOR + line
                                row = values
                                values = ""
                            }
                            else {
                                row = line
                            }
                            break
                        }
                        else {
                            values += TextFile.TERMINATOR + line
                        }
                    }
                    //head 不为空 tail 不为空
                    else if (head != "" && tail != "") {
                        if (line.startsWith(head) && line.endsWith(tail) && values == "") {
                            row = line
                            break
                        }
                        else if (line.startsWith(head) && values == "") {
                            values = line
                        }
                        else if (line.endsWith(tail) && values != "") {
                            values += TextFile.TERMINATOR + line
                            row = values
                            values = ""
                            break
                        }
                        else {
                            values += TextFile.TERMINATOR + line
                        }
                    }
                }
                else {
                    if (values != "") {
                        row = values
                        values = ""
                    }
                    break
                }
            }
        }

        row
    }

    def select(SQL: String): DataTable = {
        select(new SELECT(SQL))
    }

    def select(SELECT: SELECT): DataTable = {
        this.SELECT = SELECT

        table.clear()

        seek(SELECT.seek)

        //csv文件要跳过3个特殊字符
        if (format == TextFile.CSV && access.getFilePointer == 0) {
            var i = 0
            while (access.readByte() < 0 && i < 3) {
                i += 1
            }
            access.seek(access.getFilePointer - 1)
        }

        //转换
        SELECT.turnColumns(columns)

        //默认数据结构
        SELECT.columns.foreach(column => {
            if (column.label!= "*") {
                table.addField(column.label, column.dataType)
            }
        })

        if (first) {
            first = false
        }
        else if (skip > 0) {
            //只有从头读时才跳行
            skip = 0
        }

        values = ""
        row = 0

        if (SELECT.limit != 0) {
            var line: String = null
            do {
                line = readLine()
                if (line != null) {
                    row += 1
                    if (row == 1 && header && columns.isEmpty) {
                        line.split(separator, columns.size)
                            .foreach(item => {
                                columns += item -> DataType.TEXT
                            })
                    }
                    if (row > skip) {
                        val data = convert(line)
                        //where
                        if (data.nonEmpty && SELECT.where(data)) {
                            meet += 1
                            //limit
                            if (meet > SELECT.start && (SELECT.limit == -1 || meet <= SELECT.most)) {
                                val map = new DataRow()
                                SELECT.columns.foreach(column => {
                                    if (column.columnType == Column.CONSTANT) {
                                        map.set(column.label, column.value, column.dataType)
                                    }
                                    else if (column.label == "*") {
                                        columns.foreach(field => {
                                            map.set(field._1, data.get(field._1).orNull, field._2)
                                        })
                                    }
                                    else {
                                        map.set(column.label, data.get(column.origin).orNull, column.dataType)
                                    }
                                })
                                table.addRow(map)
                            }
                        }
                    }
                }
            }
            while (line != null && (SELECT.limit == -1 || meet <= SELECT.most))
        }

        table
    }

    private def convert(line: String): DataRow = {
        if (format == TextFile.JSON) {
            try {
                val data = Json(line).parseRow("/")
                if (columns.isEmpty) {
                    columns ++= data.columns
                }
                data
            }
            catch {
                case e: Exception =>
                    e.printStackTrace()
                    new DataRow()
            }
        }
        else {
            val items = {
                if (separator != "") {
                    if (format == TextFile.CSV) {
                        line.$split(',').toArray
                    }
                    else {
                        line.split(separator, columns.size) //按列数拆分
                    }
                }
                else {
                    Array[String](line)
                }
            }
            val data = new DataRow()

            var i = 0
            columns.foreach(column => {
                data.set(column._1,
                    if (i < items.length) {
                        column._2 match {
                            case DataType.INTEGER => items(i).toInteger(0)
                            case DataType.DECIMAL => items(i).toDecimal(0)
                            case DataType.DATETIME => items(i).toDateTimeOrElse(null)
                            case DataType.BOOLEAN => items(i).toBoolean(false)
                            case _ => items(i)
                        }
                    }
                    else {
                        null
                    }, column._2)
                i += 1
            })

            data
        }
    }

    def close(): Unit = {
        access.close()
    }
}