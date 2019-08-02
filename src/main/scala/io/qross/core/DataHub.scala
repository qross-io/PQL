package io.qross.core

import com.fasterxml.jackson.databind.JsonNode
import io.qross.ext.Output
import io.qross.core.Parameter._
import io.qross.ext.TypeExt._
import io.qross.fs.FilePath._
import io.qross.fs._
import DataType.DataType
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.net.{Email, Http, HttpClient, Json}
import io.qross.setting.Global
import io.qross.sql.{GlobalVariable, OUTPUT, PSQL}
import io.qross.thread.Parallel
import io.qross.time.{DateTime, Timer}

import scala.collection.mutable
import scala.collection.parallel.mutable.ParArray
import scala.io.Source


class DataHub () {
    
    private val SOURCES = mutable.HashMap[String, DataSource]()

    //temp database
    private val holder = s"temp_${DateTime.now.getString("yyyyMMddHHmmssSSS")}_${Math.round(Math.random() * 10000000D)}.sqlite".locate()

    private var CURRENT: DataSource = _   //current dataSource - open
    private var TARGET: DataSource = _    //current dataDestination - saveAs

    private var DEBUG = true
    
    private val TABLE = DataTable()  //current buffer
    private val BUFFER = new mutable.HashMap[String, DataTable]() //all buffer

    private var TO_BE_CLEAR: Boolean = false

    //producers/consumers amount
    private var LINES: Int = Global.CORES
    private var TANKS: Int = 3
    //selectSQL, @param
    private val pageSQLs = new mutable.HashMap[String, String]()
    //selectSQL, (@param, beginKey, endKey, blockSize)
    private val blockSQLs = new mutable.HashMap[String, (String, Long, Long, Int)]()
    //selectSQL
    private var processSQLs = new mutable.ArrayBuffer[String]()

    private var EXCEL: Excel = _
    private var EMAIL: Email = _

    private var JSON: Json = _
    private var READER: FileReader = _
    private var HTTP: Http = _
    private val ZIP = new Zip()

    private var PSQL: PSQL = _

    //全局变量-最后一次get方法的结果集数量
    private var $COUNT: Int = 0
    private var $TOTAL: Int = 0

    var userId: Int = 0
    var userName: String = "anonymous"
    var roleName: String = "WORKER"

    //打开默认数据库
    openDefault()

    // ---------- system ----------
    
    def debug(enabled: Boolean = true): DataHub = {
        DEBUG = enabled
        this
    }

    def signIn(userId: Int, userName: String, roleName: String = "WORKER"): DataHub = {
        this.userId = userId
        this.userName = userName
        this.roleName = roleName

        //从数据库加载用户全局变量
        if (JDBC.hasQrossSystem) {
            DataSource.queryDataTable("SELECT var_name, var_type, var_value FROM qross_variables WHERE var_user=?", userId)
                    .foreach(row => {
                        GlobalVariable.USER.set(
                            row.getString("var_name").toUpperCase(),
                            row.getString("var_type") match {
                                case "INTEGER" => row.getLong("var_value")
                                case "DECIMAL" => row.getDouble("var_value")
                                case _ => row.getString("var_value")
                            })

                    }).clear()
        }

        this
    }

    // ---------- open ----------
    
    def openCache(): DataHub = {
        reset()
        if (!SOURCES.contains("CACHE")) {
            SOURCES += "CACHE" -> DataSource.createMemoryDatabase
        }
        CURRENT = SOURCES("CACHE")
        this
    }

    def openTemp(): DataHub = {
        reset()
        if (!SOURCES.contains("TEMP")) {
            SOURCES += "TEMP" -> new DataSource(holder)
        }
        CURRENT = SOURCES("TEMP")
        this
    }
    
    def openDefault(): DataHub = {
        reset()
        if (!SOURCES.contains("DEFAULT")) {
            SOURCES += "DEFAULT" -> DataSource.openDefault()
        }
        CURRENT = SOURCES("DEFAULT")
        this
    }
    
    def open(connectionNameOrDataSource: Any, database: String = ""): DataHub = {
        reset()
        connectionNameOrDataSource match {
            case connectionName: String =>
                    if (!SOURCES.contains(connectionName)) {
                        SOURCES += connectionName -> new DataSource(connectionName, database)
                    }
                    CURRENT = SOURCES(connectionName)
            case dataSource: DataSource =>
                if (!SOURCES.contains(dataSource.connectionName)) {
                    SOURCES += dataSource.connectionName -> dataSource
                }
                CURRENT = dataSource
            case _ => throw new Exception("Unsupported data source parameter format, only support String or DataSource")
        }

        this
    }

    def use(databaseName: String): DataHub = {
        CURRENT.use(databaseName)
        this
    }

    //文本文件相关

    def openTextFile(path: String): DataHub = {
        READER = FileReader(path)
        this
    }

    def useDelimiter(delimiter: String): DataHub = {
        READER.delimit(delimiter)
        this
    }

    def asTable(tableName: String): DataHub = {
        READER.asTable(tableName)
        this
    }

    def withColumns(fields: String*): DataHub = {
        READER.withColumns(fields: _*)
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
    
    // ---------- save as ----------
    
    def saveAsCache(): DataHub = {
        if (!SOURCES.contains("CACHE")) {
            SOURCES += "CACHE" -> DataSource.createMemoryDatabase
        }
        TARGET = SOURCES("CACHE")
        this
    }

    def saveAsTemp(): DataHub = {
        if (!SOURCES.contains("TEMP")) {
            SOURCES += "TEMP" -> new DataSource(holder)
        }
        TARGET = SOURCES("TEMP")
        this
    }

    def saveAsDefault(): DataHub = {
        if (!SOURCES.contains("DEFAULT")) {
            SOURCES += "DEFAULT" -> DataSource.openDefault()
        }
        TARGET = SOURCES("DEFAULT")
        this
    }
    
    def saveAs(connectionName: String, database: String = ""): DataHub = {
        if (!SOURCES.contains(connectionName)) {
            SOURCES += connectionName -> new DataSource(connectionName, database)
        }
        TARGET = SOURCES(connectionName)
        this
    }

    def saveAsNewTextFile(fileNameOrFullPath: String, delimiter: String = ","): DataHub = {
        val path = fileNameOrFullPath.locate()
        FileWriter(path, true).delimit(delimiter).writeTable(TABLE).close()
        ZIP.addFile(path)
        this
    }
    
    def saveAsTextFile(fileNameOrFullPath: String, delimiter: String = ","): DataHub = {
        val path = fileNameOrFullPath.locate()
        FileWriter(path, false).delimit(delimiter).writeTable(TABLE).close()
        ZIP.addFile(path)
        this
    }

    def saveAsNewCsvFile(fileNameOrFullPath: String): DataHub = {
        val path = fileNameOrFullPath.locate()
        FileWriter(path, true).delimit(",").writeTable(TABLE).close()
        ZIP.addFile(path)
        this
    }

    def saveAsCsvFile(fileNameOrFullPath: String): DataHub = {
        val path = fileNameOrFullPath.locate()
        FileWriter(path, false).delimit(",").writeTable(TABLE).close()
        ZIP.addFile(path)
        this
    }

    def writeEmail(title: String): DataHub = {
        EMAIL = new Email(title)
        this
    }

    def fromResourceTemplate(resourceFile: String): DataHub = {
        EMAIL.fromTemplate(resourceFile)
        this
    }

    def withDefaultSignature(): DataHub = {
        EMAIL.withDefaultSignature()
        this
    }

    def withSignature(resourceFile: String): DataHub = {
        EMAIL.withSignature(resourceFile)
        this
    }

    def setEmailContent(content: String): DataHub = {
        EMAIL.setContent(content)
        this
    }

    def placeEmailTitle(title: String): DataHub = {
        EMAIL.placeContent("${title}", title)
        this
    }

    def placeEmailContent(content: String): DataHub = {
        EMAIL.placeContent("${content}", content)
        this
    }

    def placeEmailDataTable(placeHolder: String = ""): DataHub = {
        if (placeHolder == "") {
            EMAIL.placeContent("${data}", TABLE.toHtmlString)
        }
        else {
            EMAIL.placeContent("${" + placeHolder + "}", TABLE.toHtmlString)
        }

        TO_BE_CLEAR = true

        this
    }

    def placeEmailHolder(replacements: (String, String)*): DataHub = {
        for ((placeHolder, replacement) <- replacements) {
            EMAIL.placeContent(placeHolder, replacement)
        }
        this
    }

    def placeEmailContentWithRow(i: Int = 0): DataHub = {
        TABLE.getRow(i) match {
            case Some(row) => EMAIL.placeContentWidthDataRow(row)
            case None =>
        }

        TO_BE_CLEAR = true
        this
    }

    def placeEmailContentWithFirstRow(): DataHub = {
        TABLE.firstRow match {
            case Some(row) => EMAIL.placeContentWidthDataRow(row)
            case _ =>
        }

        TO_BE_CLEAR = true

        this
    }

    def to(receivers: String): DataHub = {
        EMAIL.to(receivers)
        this
    }

    def cc(receivers: String): DataHub = {
        EMAIL.cc(receivers)
        this
    }

    def bcc(receivers: String): DataHub = {
        EMAIL.bcc(receivers)
        this
    }

    def attach(path: String): DataHub = {
        EMAIL.attach(path)
        this
    }

    def send(): DataHub = {
        EMAIL.placeContent("${title}", "")
        EMAIL.placeContent("${content}", "")
        EMAIL.placeContent("${data}", "")
        EMAIL.send()
        this
    }

    def saveAsExcel(fileNameOrPath: String): DataHub = {
        EXCEL = new Excel(fileNameOrPath)
        ZIP.addFile(EXCEL.path)
        this
    }

    def saveAsNewExcel(fileNameOrPath: String): DataHub = {
        deleteFile(fileNameOrPath)
        EXCEL = new Excel(fileNameOrPath)
        ZIP.addFile(EXCEL.path)
        this
    }

    def fromExcelTemplate(templateName: String): DataHub = {
        EXCEL.fromTemplate(templateName)
        this
    }

    def appendSheet(sheetName: String = "sheet1", initialRow: Int = 0, initialColumn: Int = 0): DataHub = {

        if (TABLE.nonEmptySchema) {
            EXCEL
                .setInitialRow(initialRow)
                .setInitialColumn(0)
                .openSheet(sheetName)
                .withoutHeader()
                .appendTable(TABLE)
        }


        if (pageSQLs.nonEmpty || blockSQLs.nonEmpty) {
            stream(table => {
                EXCEL
                    .setInitialRow(initialRow)
                    .setInitialColumn(0)
                    .openSheet(sheetName)
                    .withoutHeader()
                    .appendTable(table)
                table.clear()
            })
        }

        TO_BE_CLEAR = true

        this
    }

    def appendSheetWithHeader(sheetName: String = "sheet1", initialRow: Int = 0, initialColumn: Int = 0): DataHub = {

        if (TABLE.nonEmptySchema) {
            EXCEL
                .setInitialRow(initialRow)
                .setInitialColumn(0)
                .openSheet(sheetName)
                .withHeader()
                .appendTable(TABLE)
        }

        if (pageSQLs.nonEmpty || blockSQLs.nonEmpty) {
            stream(table => {
                EXCEL
                    .setInitialRow(initialRow)
                    .setInitialColumn(0)
                    .openSheet(sheetName)
                    .withHeader()
                    .appendTable(table)
                table.clear()
            })
        }

        TO_BE_CLEAR = true

        this
    }

    def writeCellValue(value: Any, sheetName: String = "sheet1", rowIndex: Int = 0, colIndex: Int = 0): DataHub = {
        EXCEL.openSheet(sheetName).setCellValue(value, rowIndex, colIndex).setInitialRow(rowIndex).setInitialColumn(colIndex)
        this
    }

    def writeSheet(sheetName: String = "sheet1", initialRow: Int = 0, initialColumn: Int = 0): DataHub = {
        EXCEL
          .setInitialRow(initialRow)
          .setInitialColumn(0)
          .openSheet(sheetName)
          .withoutHeader()
          .writeTable(TABLE)

        TO_BE_CLEAR = true

        this
    }

    def writeSheetWithHeader(sheetName: String = "sheet1", initialRow: Int = 0, initialColumn: Int = 0): DataHub = {
        EXCEL
          .setInitialRow(initialRow)
          .setInitialColumn(0)
          .openSheet(sheetName)
          .withHeader()
          .writeTable(TABLE)

        TO_BE_CLEAR = true

        this
    }

    def setRegionStyle(rows: (Int, Int), cols: (Int, Int), styles: (String, Any)*): DataHub = {
        EXCEL.setStyle(rows, cols, 1, styles: _*)
        this
    }

    def withCellStyle(styles: (String, Any)*): DataHub = {
        EXCEL.setCellStyle(EXCEL.getInitialRow, EXCEL.getInitialColumn, styles: _*)
        this
    }

    def withHeaderStyle(styles: (String, Any)*): DataHub = {
        EXCEL.setRowStyle(
            EXCEL.getInitialRow,
            EXCEL.getInitialColumn -> (EXCEL.getInitialColumn + TABLE.getFields.size - 1),
            styles: _*)

        this
    }

    def withRowStyle(styles: (String, Any)*): DataHub = {
        EXCEL.setRowsStyle(
            EXCEL.getActualInitialRow -> (EXCEL.getActualInitialRow + TABLE.count() - 1),
            EXCEL.getInitialColumn -> (EXCEL.getInitialColumn + TABLE.getFields.size - 1),
            styles: _*)

        this
    }

    def withAlternateRowStyle(styles: (String, Any)*): DataHub = {
        EXCEL.setAlternateRowsStyle(
            EXCEL.getActualInitialRow -> (EXCEL.getActualInitialRow + TABLE.count() - 1),
            EXCEL.getInitialColumn -> (EXCEL.getInitialColumn + TABLE.getFields.size - 1),
            styles: _*
        )
        this
    }

    def mergeCells(rows: (Int, Int), cols: (Int, Int)): DataHub = {
        EXCEL.mergeRegion(rows, cols)
        this
    }

    def removeMergeRegion(firstRow: Int, firstColumn: Int): DataHub = {
        EXCEL.removeMergedRegion(firstRow, firstColumn)
        this
    }

    def postExcelTo(url: String, data: String = ""): DataHub = {
        JSON = new Json(HttpClient.postFile(url, EXCEL.path, data))
        this
    }

    //actualInitialRow - append
    //def withStyle - initialRow + 1, TABLE.rows.size, initialColumn, TABLE.columns.size
    //def withAlternateStyle - initialRow + 2, TABLE.rows.size, initialColumn, TABLE.columns.size
    //def withHeaderStyle - initialRow, initialRow, initialColumn, , TABLE.columns.size
    //def mergeCells

    def attachExcelToEmail(title: String): DataHub = {
        EMAIL = new Email(title)
        EMAIL.attach(EXCEL.fileName)
        this
    }

    def deleteFile(fileName: String): DataHub = {
        fileName.delete()
        this
    }

    def andZip(fileNameOrFullPath: String): DataHub = {
        ZIP.compress(fileNameOrFullPath.locate())
        this
    }

    def andZipAll(fileNameOrFullPath: String): DataHub = {
        ZIP.compressAll(fileNameOrFullPath.locate())
        this
    }

    def addFileToZipList(fileNameOrFullPath: String): DataHub = {
        ZIP.addFile(fileNameOrFullPath.locate())
        this
    }

    //清除列表并删除文件
    def andClear(): DataHub = {
        ZIP.deleteAll()
        this
    }

    //仅清除列表
    def resetZipList(): DataHub = {
        ZIP.clear()
        this
    }

    def showZipList(): DataHub = {
        ZIP.zipList.foreach(file => println(file.getAbsolutePath))
        this
    }

    def attachZipToEmail(title: String): DataHub = {
        EMAIL = new Email(title)
        EMAIL.attach(ZIP.zipFile)
        this
    }

    // ---------- Reset ----------

    def reset(): DataHub = {
        if (TO_BE_CLEAR) {
            TABLE.clear()
            pageSQLs.clear()
            $TOTAL = 0
            TO_BE_CLEAR = false
        }
        this
    }
    
    // ---------- cache ----------

    def cache(tableName: String): DataHub = {
        cache(tableName, "")
    }

    def cache(tableName: String, primaryKey: String): DataHub = {

        if (TABLE.nonEmptySchema) {
            cache(tableName, TABLE, primaryKey)
        }

        if (pageSQLs.nonEmpty || blockSQLs.nonEmpty) {
            stream(table => {
                cache(tableName, table, primaryKey)
                table.clear()
            })
        }

        this
    }

    def cache(tableName: String, table: DataTable): DataHub = {
        cache(tableName, table, "")
    }
    
    def cache(tableName: String, table: DataTable, primaryKey: String): DataHub = {

        if (!SOURCES.contains("CACHE")) {
            SOURCES += "CACHE" -> DataSource.createMemoryDatabase
        }

        //var createSQL = "" + tableName + " (__pid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE"
        var createSQL = s"CREATE TABLE IF NOT EXISTS $tableName ("
        if (primaryKey != "") {
            createSQL += s" $primaryKey INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, "
        }
        val placeHolders = new mutable.ArrayBuffer[String]()
        val fields = new mutable.ArrayBuffer[String]()
        for ((field, dataType) <- table.getFields) {
            fields += field + " " + dataType.toString
            placeHolders += "?"
        }
        createSQL += fields.mkString(", ") + ")"
        SOURCES("CACHE").executeNonQuery(createSQL)
        fields.clear()
    
        if (DEBUG) {
            table.show(10)
            Output.writeMessage(createSQL)
        }

        if (table.nonEmpty) {
            SOURCES("CACHE").tableUpdate("INSERT INTO " + tableName + " (" + table.getFieldNames.mkString(",") + ") VALUES (" + placeHolders.mkString(",") + ")", table)
        }
        placeHolders.clear()
        
        TO_BE_CLEAR = true
        
        this
    }

    def debugger(info: String = ""): DataHub = {
        println(info)
        println("pageSQLs")
        pageSQLs.foreach(println)
        println("blockSQLs")
        blockSQLs.foreach(println)

        this
    }

    // ---------- temp ----------

    def temp(tableName: String, keys: String*): DataHub = {

        if (TABLE.nonEmptySchema) {
            temp(tableName, TABLE, keys: _*)
        }

        if (pageSQLs.nonEmpty || blockSQLs.nonEmpty) {
            stream(table => {
                temp(tableName, table, keys: _*)
                table.clear()
            })
        }

        this
    }

    def temp(tableName: String, table: DataTable, keys: String*): DataHub = {

        /*
        keys format
        [PRIMARY [KEY]] keyName - if not exists in table, will add as primary key with auto increment - will be ignored if exists
        [KEY] keyName[,keyName2,...] - common index, fields must exists
        UNIQUE [KEY] keyName[,keyName2,...] - unique index, fields must exists
        */
        var primaryKey = ""
        val indexSQL = s"CREATE #{unique}INDEX IF NOT EXISTS idx_${tableName}_#{fields} ON $tableName (#{keys})"
        val indexes = new mutable.ArrayBuffer[String]()
        keys.foreach(key => {
            var index = key.trim()
            var unique = ""

            if (index.toUpperCase().startsWith("PRIMARY ")) {
                index = index.substring(index.indexOf(" ") + 1).trim()
            }
            if (index.toUpperCase().startsWith("UNIQUE ")) {
                index = index.substring(index.indexOf(" ") + 1).trim()
                unique = "UNIQUE "
            }
            if (index.toUpperCase().startsWith("KEY ")) {
                index = index.substring(index.indexOf(" ") + 1).trim()
            }
            index = index.replace(" ", "")

            if (!index.contains(",") && !table.contains(index)) {
                primaryKey = index
            }
            else {
                indexes += indexSQL.replace("#{unique}", unique)
                    .replace("#{fields}", index.replace(",", "_"))
                    .replace("#{keys}", index)
            }
        })

        if (!SOURCES.contains("TEMP")) {
            SOURCES += "TEMP" -> new DataSource(holder)
        }

        //var createSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (__pid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE"
        var createSQL = s"CREATE TABLE IF NOT EXISTS $tableName ("
        if (primaryKey != "") {
            createSQL += s" $primaryKey INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, "
        }
        val placeHolders = new mutable.ArrayBuffer[String]()
        val fields = new mutable.ArrayBuffer[String]()
        for ((field, dataType) <- table.getFields) {
            fields += field + " " + dataType.toString
            placeHolders += "?"
        }
        createSQL += fields.mkString(", ") + ")"
        SOURCES("TEMP").executeNonQuery(createSQL)
        fields.clear()

        if (DEBUG) {
            table.show(10)
            Output.writeMessage(createSQL)
        }

        if (table.nonEmpty) {
            SOURCES("TEMP").tableUpdate("INSERT INTO " + tableName + " (" + table.getFieldNames.mkString(",") + ") VALUES (" + placeHolders.mkString(",") + ")", table)
        }
        placeHolders.clear()

        if (indexes.nonEmpty) {
            indexes.foreach(SQL => {
                println(SQL)
                SOURCES("TEMP").executeNonQuery(SQL)
            })
        }

        TO_BE_CLEAR = true

        this
    }

    // ---------- base method ----------

    //execute SQL on target dataSource
    def set(nonQuerySQL: String, values: Any*): DataHub = {
        if (DEBUG) {
              println(nonQuerySQL)
          }

        CURRENT.executeNonQuery(nonQuerySQL, values: _*)
        this
    }

    //keyword table name must add tow type quote like "`case`" in presto
    def select(selectSQL: String, values: Any*): DataHub = get(selectSQL, values: _*)
    def get(selectSQL: String, values: Any*): DataHub = {
        //clear if new transaction
        reset()

        if (DEBUG) {
            println(selectSQL)
        }

        TABLE.merge(CURRENT.executeDataTable(selectSQL, values: _*))
        $TOTAL += TABLE.count()
        $COUNT = TABLE.count()

        this
    }

    def COUNT: Int = {
        $COUNT
    }

    def TOTAL: Int = {
        $TOTAL
    }

    //设置并行度
    def pipes(amount: Int): DataHub = {
        LINES = amount
        this
    }

    //设置缓冲区数量
    def tanks(amount: Int): DataHub = {
        TANKS = amount
        this
    }

    //按limit分页
    //select * from table limit @offset, 10000  多线程
    //select * from table where id>@id LIMIT 10000  单线程
    def page(selectSQL: String): DataHub = {
        //clear if new transaction
        reset()

        if (DEBUG) {
            println(selectSQL)
        }

        selectSQL.matchParameters.headOption match {
            case Some(param) => pageSQLs += selectSQL -> param.group(0)
            case None => TABLE.merge(CURRENT.executeDataTable(selectSQL))
        }

        this
    }

    //按主键分块读取
    //begin_id, end_id
    //每10000行一块

    //select * from table where id>@id AND id<=@id
    //select id from table order by id asc limit 1
    //select max(id) from table
    //beginKeyOrSQL为整数时左开右闭, 为SQL时左闭右闭
    def block(selectSQL: String, beginKeyOrSQL: Any, endKeyOrSQL: Any, blockSize: Int = 10000): DataHub = {

        reset()

        if (DEBUG) {
            println(selectSQL)
        }

        selectSQL.matchParameters.headOption match {
            case Some(param) =>
                blockSQLs += selectSQL -> (param.group(0),
                        beginKeyOrSQL match {
                            case sentence: String => CURRENT.executeDataRow(sentence).getFirstLong()
                            case key: Long => key
                            case key: Int => key.toLong
                            case _ => beginKeyOrSQL.toString.toLong
                        },
                        endKeyOrSQL match {
                            case sentence: String => CURRENT.executeDataRow(sentence).getFirstLong()
                            case key: Long => key
                            case key: Int => key.toLong
                            case _ => endKeyOrSQL.toString.toLong
                        },
                        blockSize)
            case None => TABLE.merge(CURRENT.executeDataTable(selectSQL))
        }

        this
    }

    //多线程执行同一条更新语句, beginKeyOrSQL为整数时左开右闭, 为SQL时左闭右闭
    //初始设计目的是解决tidb不能大批量更新的问题, 只能按块更新
    def bulk(nonQuerySQL: String, beginKeyOrSQL: Any, endKeyOrSQL: Any, bulkSize: Int = 100000): DataHub = {

        if (DEBUG) {
            println(nonQuerySQL)
        }

        nonQuerySQL.matchParameters.headOption match {
            case Some(param) =>
                    var begin = beginKeyOrSQL match {
                        case sentence: String => CURRENT.executeDataRow(sentence).getFirstLong() - 1
                        case key: Int => key.toLong
                        case key: Long => key
                        case _ => beginKeyOrSQL.toString.toLong
                    }
                    val end = endKeyOrSQL match {
                        case sentence: String => CURRENT.executeDataRow(sentence).getFirstLong()
                        case key: Int => key.toLong
                        case key: Long => key
                        case _ => beginKeyOrSQL.toString.toLong
                    }

                    while (begin < end) {
                        Bulker.QUEUE.add(nonQuerySQL.replaceFirst(param.group(0), begin.toString).replace(param.group(0), (if (begin + bulkSize >= end) end else begin + bulkSize).toString))
                        begin += bulkSize
                    }

                val parallel = new Parallel()
                //producer
                for (i <- 0 until LINES) {
                    parallel.add(new Bulker(CURRENT))
                }
                parallel.startAll()
                parallel.waitAll()

                //blockSize
            case None => CURRENT.executeNonQuery(nonQuerySQL)
        }

        this
    }


    def join(selectSQL: String, on: (String, String)*): DataHub = {
        TABLE.join(CURRENT.executeDataTable(selectSQL), on: _*)
        this
    }


    def pass(querySentence: String, default:(String, Any)*): DataHub = {
        if (DEBUG) println(querySentence)
        if (TABLE.isEmpty) {
            if (default.nonEmpty) {
                TABLE.addRow(DataRow(default: _*))
            }
            else {
                throw new Exception("No data to pass. Please ensure data exists or provide default value")
            }
        }
        TABLE.cut(CURRENT.tableSelect(querySentence, TABLE))

        this
    }

    //1. get 单线程生产
    //2. pass 单线程加工
    //3. put 单线程消费
    //4. page/block 多线程生产
    //5. process 多线程加工
    //6. batch 多线程消费

    // #1+3 生产->消费
    // #1+2+3 生产->传递->消费
    // #4+3 多线程生产单线程消费
    // #4+5+3 多线程生产多线程加工
    // #4+6 多线程生产多线程消费
    // #4+5+6 多线程加工多线程消费

    def process(selectSQL: String): DataHub = {
        processSQLs += selectSQL
        this
    }
    
    //execute SQL on target dataSource
    def prep(nonQuerySQL: String, values: Any*): DataHub = {
        TARGET.executeNonQuery(nonQuerySQL, values: _*)
        this
    }
   
    def insert(insertSQL: String): DataHub = put(insertSQL)
    def update(updateSQL: String): DataHub = put(updateSQL)
    def delete(deleteSQL: String): DataHub = put(deleteSQL)
    def put(nonQuerySQL: String): DataHub = {

        if (DEBUG) {
            TABLE.show(10)
            println(nonQuerySQL)
        }

        if (TABLE.nonEmpty) {
            TARGET.tableUpdate(nonQuerySQL, TABLE)
        }

        if (pageSQLs.nonEmpty || blockSQLs.nonEmpty) {
            stream(table => {
                TARGET.tableUpdate(nonQuerySQL, table)
                table.clear()
            })
        }

        TO_BE_CLEAR = true

        this
    }

    def insert(insertSQL: String, table: DataTable): DataHub = put(insertSQL, table)
    def update(updateSQL: String, table: DataTable): DataHub = put(updateSQL, table)
    def delete(deleteSQL: String, table: DataTable): DataHub = put(deleteSQL, table)
    def put(nonQuerySentence: String, table: DataTable): DataHub = {

        if (DEBUG) {
            println(nonQuerySentence)
        }

        TARGET.tableUpdate(nonQuerySentence, table)
        this
    }

    // #4+6 多线程生产多线程消费
    // #4+5+6 多线程加工多线程消费
    def batch(nonQuerySentence: String): DataHub = {

        //pageSQLs or blockSQLs
        //processSQLs
        //batchSQL

        val parallel = new Parallel()

        //生产者
        for ((selectSQL, param) <- pageSQLs) {
            if (param.toLowerCase().contains("offset")) {
                //offset
                val pageSize = "\\d+".r.findFirstMatchIn(selectSQL.substring(selectSQL.indexOf(param) + param.length)) match {
                    case Some(ps) => ps.group(0).toInt
                    case None => 10000
                }

                //producer
                for (i <- 0 until LINES) { //Global.CORES
                    parallel.add(new Pager(CURRENT, selectSQL, param, pageSize, TANKS))
                }
            }
        }

        //生产者
        for ((selectSQL, config) <- blockSQLs) {
            //SELECT * FROM table WHERE id>@id AND id<=@id;

            var i = if (config._2 == 0) 0 else config._2 - 1
            while (i < config._3) {
                var SQL = selectSQL
                SQL = SQL.replaceFirst(config._1, i.toString)
                i += config._4
                if (i > config._3) {
                    i = config._3
                }
                SQL = SQL.replaceFirst(config._1, i.toString)
                Blocker.QUEUE.add(SQL)
            }

            //producer
            for (i <- 0 until LINES) { //Global.CORES
                parallel.add(new Blocker(CURRENT))
            }
        }

        //中间多个加工者
        for (index <- processSQLs.indices) {
            for (i <- 0 until LINES) {
                parallel.add(new Processer(CURRENT, processSQLs(index), index, TANKS))
            }
        }

        //最终消费者
        for (i <- 0 until LINES) {
            parallel.add(new Batcher(TARGET, nonQuerySentence))
        }

        parallel.startAll()
        parallel.waitAll()

        pageSQLs.clear()
        blockSQLs.clear()
        processSQLs.clear()

        Processer.clear()

        this
    }

    //dataSource: target dataSource
    private def stream(handler: DataTable => Unit): Unit = {

        for ((selectSQL, param) <- pageSQLs) {
            if (param.toLowerCase().contains("offset")) {
                //offset
                val pageSize = "\\d+".r.findFirstMatchIn(selectSQL.substring(selectSQL.indexOf(param) + param.length)) match {
                    case Some(ps) => ps.group(0).toInt
                    case None => 10000
                }

                val parallel = new Parallel()
                //producer
                for (i <- 0 until LINES) {  //Global.CORES
                    parallel.add(new Pager(CURRENT, selectSQL, param, pageSize, TANKS))
                }
                parallel.startAll()
                //consumer
                while(Pager.DATA.size() > 0 || parallel.running) {
                    val table = Pager.DATA.poll()
                    if (table != null) {
                        $TOTAL += table.count()
                        $COUNT = table.count()
                        handler(table)
                        Output.writeMessage(s"$pageSize SAVED")
                    }
                    Timer.sleep(0.1F)
                }
                parallel.waitAll()
                Pager.CUBE.reset()
                Output.writeMessage("Exit All Page")

                /* origin code for single thread
                var page = 0
                var continue = true
                do {
                    val table = CURRENT.executeDataTable(selectSQL.replace("@" + param, String.valueOf(page * pageSize)))
                    if (table.nonEmpty) {
                        handler(table)
                        //dataSource.tableUpdate(nonQuerySQL, table)
                        page += 1
                    }
                    else {
                        continue = false
                    }
                }
                while(continue)
                */
            }
            else {
                //SELECT * FROM table WHERE id>@id LIMIT 10000;

                var id = 0L //primaryKey
                var continue = true
                do {
                    val table = CURRENT.executeDataTable(selectSQL.replace(param, String.valueOf(id)))
                    $TOTAL += table.count()
                    $COUNT = table.count()
                    if (table.nonEmpty) {
                        //dataSource.tableUpdate(nonQuerySQL, table)
                        if (table.contains(param)) {
                            id = table.max(param).firstCellLongValue
                        }
                        else {
                            throw new Exception("Result set must contains primiary key: " + param)
                        }

                        handler(table)
                    }
                    else {
                        continue = false
                    }
                    table.clear()
                }
                while (continue)
            }
        }

        for ((selectSQL, config) <- blockSQLs) {
            //id

            //SELECT * FROM table WHERE id>@id AND id<=@id;

            //producer
            var i = if (config._2 == 0) 0 else config._2 - 1
            while (i < config._3) {
                var SQL = selectSQL
                SQL = SQL.replaceFirst(config._1, i.toString)
                i += config._4
                if (i > config._3) {
                    i = config._3
                }
                SQL = SQL.replaceFirst(config._1, i.toString)
                Blocker.QUEUE.add(SQL)
            }

            //consumer
            val parallel = new Parallel()
            //producer
            for (i <- 0 until LINES) {  //Global.CORES
                parallel.add(new Blocker(CURRENT, TANKS))
            }
            parallel.startAll()
            //consumer
            while(!Blocker.CUBE.closed || Blocker.DATA.size() > 0 || parallel.running) {
                val table = Blocker.DATA.poll()
                if (table != null) {
                    $TOTAL += table.count()
                    $COUNT = table.count()
                    handler(table)
                    Output.writeMessage(s"${config._4} SAVED")
                }
                Timer.sleep(0.1F)
            }
            Output.writeMessage("Exit Block While")
            parallel.waitAll()
            Output.writeMessage("Exit All Block")
        }
    }
    
    // ---------- buffer basic ----------
    
    //switch table
    def from(tableName: String): DataHub = {
        TABLE.clear()
        
        if (BUFFER.contains(tableName)) {
            TABLE.union(BUFFER(tableName))
        }
        else {
            throw new Exception(s"There is no table named $tableName in buffer.")
        }
        this
    }
    
    def buffer(tableName: String, table: DataTable): DataHub = {
        BUFFER += tableName -> table
        TABLE.copy(table)
        this
    }
    
    def buffer(table: DataTable): DataHub = {
        TABLE.copy(table)
        this
    }
    
    def buffer(tableName: String): DataHub = {
        BUFFER += tableName -> DataTable.from(TABLE)
        this
    }
    
    def merge(table: DataTable): DataHub = {
        TABLE.merge(table)
        this
    }
    
    def merge(tableName: String, table: DataTable): DataHub = {
        if (BUFFER.contains(tableName)) {
            BUFFER(tableName).merge(table)
        }
        else {
            BUFFER += tableName -> table
        }
        this
    }
    
    def union(table: DataTable): DataHub = {
        TABLE.union(table)
        this
    }
    
    def union(tableName: String, table: DataTable): DataHub = {
        if (BUFFER.contains(tableName)) {
            BUFFER(tableName).union(table)
        }
        else {
            BUFFER += tableName -> table
        }
        this
    }
    
    def takeOut(): DataTable = {
        val table = DataTable.from(TABLE)
        TABLE.clear()
        table
    }

    def containsBuffer(tableName: String): Boolean = {
        BUFFER.contains(tableName)
    }
    
    def takeOut(tableName: String): DataTable = {
        if (BUFFER.contains(tableName)) {
            val table = DataTable.from(BUFFER(tableName))
            BUFFER.remove(tableName)
            table
        }
        else {
            DataTable()
        }
    }

    def par: ParArray[DataRow] = {
        TABLE.par
    }

    def firstRow: DataRow = {
        TABLE.firstRow match {
            case Some(row) => row
            case None => DataRow()
        }
    }

    def lastRow: DataRow = {
        TABLE.lastRow match {
            case Some(row) => row
            case None => DataRow()
        }
    }

    def getRow(i: Int): DataRow = {
        TABLE.getRow(i) match {
            case Some(row) => row
            case None => DataRow()
        }
    }

    def getFirstCellStringValue(defaultValue: String = ""): String = {
        TABLE.getFirstCellStringValue(defaultValue)
    }

    def getFirstCellIntValue(defaultValue: Int = 0): Int = {
        TABLE.getFirstCellIntValue(defaultValue)
    }

    def getFirstCellLongValue(defaultValue: Long = 0L): Long = {
        TABLE.getFirstCellLongValue(defaultValue)
    }

    def getFirstCellFloatValue(defaultValue: Float = 0F): Float = {
        TABLE.getFirstCellFloatValue(defaultValue)
    }

    def getFirstCellDoubleValue(defaultValue: Double = 0D): Double = {
        TABLE.getFirstCellDoubleValue(defaultValue)
    }

    def getFirstCellBooleanValue(defaultValue: Boolean = false): Boolean = {
        TABLE.getFirstCellBooleanValue(defaultValue)
    }

    def firstCellStringValue: String = TABLE.getFirstCellStringValue()
    def firstCellIntValue: Int = TABLE.getFirstCellIntValue()
    def firstCellLongValue: Long = TABLE.getFirstCellLongValue()
    def firstCellFloatValue: Float = TABLE.getFirstCellFloatValue()
    def firstCellDoubleValue: Double = TABLE.getFirstCellDoubleValue()
    def firstCellBooleanValue: Boolean = TABLE.getFirstCellBooleanValue()
    
    def discard(tableName: String): DataHub = {
        if (BUFFER.contains(tableName)) {
            BUFFER.remove(tableName)
        }
        this
    }
    
    def nonEmpty: Boolean = {
        TABLE.nonEmpty
    }
    
    def isEmpty: Boolean = {
        TABLE.isEmpty
    }

    def show(limit: Int = 20): DataHub = {
        if (TABLE.nonEmpty) {
            TABLE.show(limit)
        }
        else {
            println("------------------------------------------------------------------------")
            println("NO DATA IN BUFFER TO SHOW")
            println("------------------------------------------------------------------------")
        }

        TO_BE_CLEAR = true

        this
    }
    
    // ---------- buffer action ----------
    
    def label(alias: (String, String)*): DataHub = {
        TABLE.label(alias: _*)
        this
    }

    def foreach(callback: DataRow => Unit): DataHub = {
        TABLE.foreach(callback)
        this
    }
    
    def map(callback: DataRow => DataRow) : DataHub = {
        TABLE.cut(TABLE.map(callback))
        this
    }
    
    def table(fields: (String, DataType)*)(callback: DataRow => DataTable): DataHub = {
        TABLE.cut(TABLE.table(fields: _*)(callback))
        this
    }
    
    def flat(callback: DataTable => DataRow): DataHub = {
        val row = callback(TABLE)
        TABLE.clear()
        TABLE.addRow(row)
        this
    }
    
    def filter(callback: DataRow => Boolean): DataHub = {
        TABLE.cut(TABLE.filter(callback))
        this
    }
    
    def collect(filter: DataRow => Boolean)(map: DataRow => DataRow): DataHub = {
        TABLE.cut(TABLE.collect(filter)(map))
        this
    }
    
    def distinct(fieldNames: String*): DataHub = {
        TABLE.cut(TABLE.distinct(fieldNames: _*))
        this
    }
    
    def count(groupBy: String*): DataHub = {
        TABLE.cut(TABLE.count(groupBy: _*))
        this
    }
    
    def sum(fieldName: String, groupBy: String*): DataHub = {
        TABLE.cut(TABLE.sum(fieldName, groupBy: _*))
        this
    }
    
    def avg(fieldName: String, groupBy: String*): DataHub = {
        TABLE.cut(TABLE.avg(fieldName, groupBy: _*))
        this
    }
    
    def min(fieldName: String, groupBy: String*): DataHub = {
        TABLE.cut(TABLE.min(fieldName, groupBy: _*))
        this
    }
    
    def max(fieldName: String, groupBy: String*): DataHub = {
        TABLE.cut(TABLE.max(fieldName, groupBy: _*))
        this
    }
    
    def take(amount: Int): DataHub = {
        TABLE.cut(TABLE.take(amount))
        this
    }

    def takeSample(amount: Int): DataHub = {
        TABLE.cut(TABLE.takeSample(amount))
        this
    }
    
    def insertRow(fields: (String, Any)*): DataHub = {

        if (TO_BE_CLEAR) {
            TABLE.clear()
            TO_BE_CLEAR = false
        }

        TABLE.insertRow(fields: _*)
        this
    }
    
    def insertRowIfEmpty(fields: (String, Any)*): DataHub = {

        if (TO_BE_CLEAR) {
            TABLE.clear()
            TO_BE_CLEAR = false
        }

        if (TABLE.isEmpty) {
            TABLE.insertRow(fields: _*)
        }
        this
    }

    def getColumn(fieldName: String): List[Any] = {
        TABLE.getColumn(fieldName)
    }
    
    def clear(): DataHub = {
        TABLE.clear()
        this
    }
    
    //def updateRow
    
    // ---------- Json & Api ---------
    
    def openJson(): DataHub = {
        this
    }
    
    def openJson(jsonText: String): DataHub = {
        JSON = Json.fromText(jsonText)
        this
    }
    
    def openJsonApi(url: String): DataHub = {
        JSON = Json.fromURL(url)
        this
    }
    
    def openJsonApi(url: String, post: String): DataHub = {
        JSON = Json.fromURL(url, post)
        this
    }

    def parse(jsonPath: String): DataHub = {
        TABLE.copy(JSON.parseTable(jsonPath))
        this
    }

    //---------- HTTP ----------

    def GET(url: String): DataHub = {
        HTTP = Http.GET(url)
        this
    }

    def POST(url: String, data: String = ""): DataHub = {
        HTTP = Http.POST(url, data)
        this
    }

    def PUT(url: String, data: String = ""): DataHub = {
        HTTP = Http.PUT(url, data)
        this
    }

    def DELETE(url: String): DataHub = {
        HTTP = Http.DELETE(url)
        this
    }

    def setHeader(name: String, value: String): DataHub = {
        HTTP.setHeader(name, value)
        this
    }

    def toJson: DataHub = {
        JSON = HTTP.toJson
        this
    }

    //---------- PSQL ----------

    def openSQL(SQL: String): DataHub = {
        PSQL = new PSQL(SQL, this)
        this
    }

    def openFileSQL(filePath: String): DataHub = {
        PSQL = new PSQL(Source.fromFile(filePath.locate()).mkString, this)
        this
    }

    def openResourceSQL(resourcePath: String): DataHub = {
        PSQL = new PSQL(ResourceFile.open(resourcePath).output, this)
        this
    }

    def setArgs(args: Any): DataHub = {
        PSQL.assign(args)
        this
    }

    def setVariable(name: String, value: Any): DataHub = {
        PSQL.set(name, value)
        this
    }

    def run(): PSQL = {
        PSQL.$run()
    }

    def run(SQL: String): PSQL = {
        new PSQL(SQL, this).$run()
    }

    def runFileSQL(filePath: String, outputType: String = OUTPUT.TABLE): PSQL = {
        new PSQL(Source.fromFile(filePath.locate()).mkString, this).$run()
    }

    def runResourceSQL(resourcePath: String, outputType: String = OUTPUT.TABLE): PSQL = {
        new PSQL(ResourceFile.open(resourcePath).output, this).$run()
    }

    // ---------- dataSource ----------

    def executeDataTable(SQL: String, values: Any*): DataTable = CURRENT.executeDataTable(SQL, values: _*)
    def executeDataRow(SQL: String, values: Any*): DataRow = CURRENT.executeDataRow(SQL, values: _*)
    def executeHashMap(SQL: String, values: Any*): java.util.Map[String, Any] = CURRENT.executeHashMap(SQL, values: _*)
    def executeMapList(SQL: String, values: Any*): java.util.List[java.util.Map[String, Any]] = CURRENT.executeMapList(SQL, values: _*)
    def executeSingleList(SQL: String, values: Any*): java.util.List[Any] = CURRENT.executeSingleList(SQL, values: _*)
    def executeSingleValue(SQL: String, values: Any*): DataCell = CURRENT.executeSingleValue(SQL, values: _*)
    def executeExists(SQL: String, values: Any*): Boolean = CURRENT.executeExists(SQL, values: _*)
    def executeNonQuery(SQL: String, values: Any*): Int = CURRENT.executeNonQuery(SQL, values: _*)
    
    // ---------- Json Basic ----------
    
    def parseTable(jsonPath: String): DataTable = JSON.parseTable(jsonPath)
    def parseRow(jsonPath: String): DataRow = JSON.parseRow(jsonPath)
    def parseList(jsonPath: String): java.util.List[Any] = JSON.parseJavaList(jsonPath)
    def parseValue(jsonPath: String): DataCell = JSON.parseValue(jsonPath)
    def parseNode(jsonPath: String): JsonNode = JSON.findNode(jsonPath)
    
    // ---------- other ----------
    
    def runCommand(commandText: String): DataHub = {
        commandText.bash()
        this
    }
    
    def close(): Unit = {

        SOURCES.values.foreach(_.close())
        SOURCES.clear()
        BUFFER.clear()
        TABLE.clear()
        pageSQLs.clear()
        blockSQLs.clear()
        processSQLs.clear()

        holder.delete()
    }
}