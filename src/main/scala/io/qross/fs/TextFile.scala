package io.qross.fs

import java.io.RandomAccessFile

import io.qross.core.{DataHub, DataRow, DataTable, DataType}
import io.qross.exception.{ColumnNotFoundException, IncorrectDataSourceException}
import io.qross.ext.TypeExt._
import io.qross.fs.Path._
import io.qross.net.Json
import io.qross.setting.Global

import scala.collection.mutable

object TextFile {

    val TXT = 1
    val CSV = 2
    val JSON = 3
    val GZ = 4
    val FILE = "file"
    val STREAM = "stream"

    val TERMINATOR: String = System.getProperty("line.separator")

    implicit class DataHub$TextFile(val dh: DataHub) {

        //private var READER: FileReader = _

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

        def openJsonFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.JSON).bracketedBy("{", "}"))
            dh
        }

        def openCsvFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.CSV).delimitedBy(","))
            dh
        }

        def openGzFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.GZ))
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
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.JSON, TextFile.FILE, true))
            dh
        }

        def saveToJsonFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.JSON, TextFile.FILE, false))
            dh
        }

        def saveAsStreamJsonFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.JSON, TextFile.STREAM))
            dh
        }

        def saveAsTextFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.TXT, TextFile.FILE, true))
            dh
        }

        def saveToTextFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.TXT, TextFile.FILE, false))
            dh
        }

        def saveAsStreamFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.TXT, TextFile.STREAM))
            dh
        }

        def saveAsCsvFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.CSV, TextFile.FILE, true))
            dh
        }

        def saveToCsvFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.CSV, TextFile.FILE, false))
            dh
        }

        def saveAsStreamCsvFile(fileNameOrFullPath: String): DataHub = {
            dh.FQL.create(fileNameOrFullPath, new TextFile(fileNameOrFullPath, TextFile.CSV, TextFile.STREAM))
            dh
        }

        //适用于saveDestination中的分隔符修改
        def delimit(delimiter: String): DataHub = {
            dh.FQL.getTable match {
                case file: TextFile => file.writer.delimit(delimiter)
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
                case file: TextFile =>
                    file.writer.writeTable(dh.getData, dh.pick[Boolean]("WITH_HEADERS").getOrElse(true))
                    file.writer.close()

                    if (dh.slots("ZIP")) {
                        dh.pick[Zip]("ZIP").orNull.addFile(file.writer.file.getAbsolutePath)
                    }
                case _ => throw new IncorrectDataSourceException("Must use SAVE sentence to save file first.")
            }
            dh
        }
    }
}

class TextFile(val fileNameOrPath: String, val format: Int, outputType: String, deleteIfExists: Boolean = false) {

    private lazy val access = new RandomAccessFile(fileNameOrPath.locate(), "r") //read
    private lazy val reader = new FileReader(fileNameOrPath, format) //read gz
    private lazy val writer = new FileWriter(fileNameOrPath, format, outputType, deleteIfExists) //write

    private var row = 0 //行号
    private var skip = 0 //如果是从头读, 略过多少行
    private var meet = 0 //满足条件的行数
    private var start = 0 //limit 起始行, 从0开始
    private var most = -1 //limit 限制行, 达到limit时停止
    private var head = "" //row head mark
    private var tail = "" //row tail mark
    private var separator = if (format == TextFile.CSV) "," else "" //row delimiter
    private val columns = new mutable.LinkedHashMap[String, DataType]()
    // label, value/name, constant/map, dataType
    private val fields = new mutable.ArrayBuffer[(String, Any, String, DataType)]()

    private var values = "" //中间变量
    private val table = new DataTable()  //result

    def this(fileNameOrPath: String) {
        this(fileNameOrPath, TextFile.TXT, TextFile.FILE)
    }

    def this(fileNameOrPath: String, format: Int) {
        this(fileNameOrPath, format, TextFile.FILE)
    }

    def this(fileNameOrPath: String, format: Int, deleteIfExists: Boolean) {
        this(fileNameOrPath, format, TextFile.FILE, deleteIfExists)
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

    def seek(position: Long): Unit = access.seek(position)

    def where(conditions: String): Unit = ???

    def limit(m: Int): TextFile = {
        start = 0
        most = m
        this
    }
    def limit(m: Int, n: Int): TextFile = {
        start = m
        most = n
        this
    }

    def orderBy(rule: String): Unit = ???

    def select(cols: (String, String)*): DataTable = {

        table.clear()
        fields.clear()

        cols.foreach(col => {
//            if (col._1 == "*") {
//                columns.foreach(column => {
//                    fields += ((column._1, column._1, "MAP", column._2))
//                })
//            }
//            else
            if (col._1.quotesWith("'") || col._1.quotesWith("\"")) {
                fields += ((col._2, col._1.removeQuotes(), "CONSTANT", DataType.TEXT))
            }
            else if ("""^-?\d+$""".r.test(col._1)) {
                fields += ((col._2, col._1.toInteger, "CONSTANT", DataType.INTEGER))
            }
            else if ("""^-?\d+\.\d+$""".r.test(col._1)) {
                fields += ((col._2, col._1.toDecimal, "CONSTANT", DataType.DECIMAL))
            }
            else if ("(?i)^true|false$".r.test(col._1)) {
                fields += ((col._2, col._1.toBoolean(false), "CONSTANT", DataType.BOOLEAN))
            }
            else if ("""(?)^null$""".r.test(col._1)) {
                fields += ((col._2, null, "CONSTANT", DataType.TEXT))
            }
            else if (columns.contains(col._1)) {
                fields += ((col._2, col._1, "MAP", columns(col._1)))
            }
            else {
                fields += ((col._2, col._1, "MAP", DataType.TEXT))
            }
        })

        //只有从头读时才跳行
        if (cursor > 0 && skip > 0) {
            skip = 0
        }

        values = ""
        row = 0

        if (format != TextFile.GZ) {
            var line: String = null
            do {
                line = access.readLine()
                if (line != null) {
                    recognize(new String(line.getBytes("ISO-8859-1"), Global.CHARSET))
                }
            }
            while (line != null && (most == -1 || meet <= start + most))
        }
        else {
            while(reader.hasNextLine && (most == -1 || meet <= start + most)) {
                recognize(reader.readLine())
            }
        }

        //收尾
        parse(values)

        table
    }

    private def recognize(line: String): Unit = {

        //head 为空 tail 为空，读一行是一行
        if (head == "" && tail == "") {
            parse(line)
        }
        //head 不为空 tail 为空
        else if (head != "" && tail == "") {
            if (line.startsWith(head)) {
                if (values != "") {
                    parse(values)
                }
                //新一行
                values = line
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
                    parse(values)
                    values = ""
                }
                else {
                    parse(line)
                }
            }
            else {
                values += TextFile.TERMINATOR + line
            }
        }
        //head 不为空 tail 不为空
        else if (head != "" && tail != "") {
            if (line.startsWith(head) && line.endsWith(tail) && values == "") {
                parse(line)
            }
            else if (line.startsWith(head) && values == "") {
                values = line
            }
            else if (line.endsWith(tail) && values != "") {
                values += TextFile.TERMINATOR + line
                parse(values)
                values = ""
            }
            else {
                values += TextFile.TERMINATOR + line
            }
        }
    }

    private def convert(line: String): DataRow = {
        if (format == TextFile.JSON) {
            val data = Json(line).parseRow("/")
            if (columns.isEmpty) {
                columns ++= data.columns
            }
            data
        }
        else {
            val items = line.split(separator, -1)
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

    private def parse(line: String): Unit = {
        if (line != "") {
            row += 1
            if (row == 1 && skip > 0 && columns.isEmpty) {
                line.split(separator, -1)
                    .foreach(item => {
                        columns += item -> DataType.TEXT
                    })
            }
            if (row > skip) {
                val data = convert(line)
                //where
                if (true) {
                    meet += 1
                    //limit
                    if (meet > start && (most == - 1 || meet <= start + most)) {
                        val map = new DataRow()
                        fields.foreach(field => {
                            if (field._3 == "CONSTANT") {
                                map.set(field._1, field._2, field._4)
                            }
                            else if (field._1 == "*") {
                                columns.foreach(column => {
                                    map.set(column._1, data.get(column._1).orNull, column._2)
                                })
                            }
                            else {
                                map.set(field._1, data.get(field._2.asInstanceOf[String]).orNull, field._4)
                            }
                        })
                        table.addRow(map)
                    }
                }
            }
        }
    }

    def close(): Unit = {
        if (format != TextFile.GZ) {
            access.close()
        }
        else {
            reader.close()
        }
        writer.close()
    }
}