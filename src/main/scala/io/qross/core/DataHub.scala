package io.qross.core

import io.qross.core.Parameter._
import io.qross.exception.{DefineAliasException, NoDataDestinationException, NoDataSourceException, OpenDataSourceException, WrongSourceNameException}
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.fql.FQL
import io.qross.fs.{Excel, FileReader, FileWriter, TextFile}
import io.qross.fs.Path._
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.pql.Patterns
import io.qross.setting.{Environment, Global, Properties}
import io.qross.thread.Parallel
import io.qross.time.{DateTime, Timer}
import javax.xml.bind.annotation.XmlElementDecl.GLOBAL

import scala.collection.mutable
import scala.collection.parallel.mutable.ParArray


object DataHub {
    def QROSS: DataHub = new DataHub(JDBC.QROSS)
    def DEFAULT: DataHub = new DataHub(JDBC.DEFAULT)
}

class DataHub (val defaultConnectionName: String) {
    
    private[qross] val SOURCES = mutable.HashMap[String, Any]()
    private[qross] val ALIASES = new mutable.HashMap[String, String]()

    if (defaultConnectionName != "") {
        SOURCES += "DEFAULT" -> new DataSource(defaultConnectionName)
    }
    else if (Properties.contains(JDBC.DEFAULT)) {
        SOURCES += "DEFAULT" ->  new DataSource(JDBC.DEFAULT)
    }

    //虚库
    private[qross] lazy val FQL = new FQL(this)

    private var lastSourceName = ""
    private[qross] var currentSourceName = if (SOURCES.contains("DEFAULT")) "DEFAULT" else ""  //current dataSource - open
    private[qross] var currentDestinationName = if (SOURCES.contains("DEFAULT")) "DEFAULT" else ""  //current dataDestination - saveTo

    private var DEBUG: Boolean = Global.DEBUG

    private val HOLDER = s"temp_${DateTime.now.getString("yyyyMMddHHmmssSSS")}_${Math.round(Math.random() * 10000000D)}.sqlite".locate()
    private val TABLE = new DataTable()  //current buffer
    private val BUFFER = new mutable.HashMap[String, DataTable]() //all buffer

    private var TO_BE_CLEAR: Boolean = false

    //扩展插槽
    private val SLOTS: mutable.HashMap[String, Any] = new mutable.HashMap[String, Any]()

    var COUNT_OF_LAST_GET: Int = 0 //最后一次get方法的结果集数量, 也包括 page, block的最后一次GET
    var TOTAL_COUNT_OF_RECENT_GET: Int = 0 //最近所有get, page, block的结果集总量, put时重置
    var AFFECTED_ROWS_OF_LAST_PUT: Int = 0
    var TOTAL_AFFECTED_ROWS_OF_RECENT_PUT: Int = 0
    var AFFECTED_ROWS_OF_LAST_SET: Int = 0
    var AFFECTED_ROWS_OF_LAST_PREP: Int = 0

    //producers/consumers amount
    private var LINES: Int = Environment.cpuThreads
    private var TANKS: Int = 3
    //selectSQL, (@param, initialPoint)
    private lazy val pageSQLs = new mutable.HashMap[String, String]()
    //selectSQL, (@param, beginKey, endKey, blockSize)
    private lazy val blockSQLs = new mutable.HashMap[String, (String, Long, Long, Int)]()
    //selectSQL
    private lazy val processSQLs = new mutable.ArrayBuffer[String]()

    def this() {
        this("")
    }

    def currentSource[T]: T = Source[T](currentSourceName)
    def currentDestination[T]: T = Source[T](currentDestinationName)
    def getSource: Any = {
        if (currentSourceName != "") {
            if (SOURCES.contains(currentSourceName)) {
                SOURCES(currentSourceName)
            }
            else {
                SOURCES(ALIASES(currentSourceName.toLowerCase()))
            }
        }
        else {
            throw new NoDataSourceException("You must open a data source first.")
        }
    }
    def getDestination: Any = {
        if (currentSourceName != "") {
            if (SOURCES.contains(currentDestinationName)) {
                SOURCES(currentDestinationName)
            }
            else {
                SOURCES(ALIASES(currentDestinationName.toLowerCase()))
            }
        }
        else {
            throw new NoDataDestinationException("You must specify a data destination first using method 'saveTo' or SAVE sentence.")
        }
    }
    def Source[T](sourceName: String): T = {
        if (SOURCES.contains(sourceName)) {
            SOURCES(sourceName).asInstanceOf[T]
        }
        else {
            SOURCES(ALIASES(sourceName.toLowerCase())).asInstanceOf[T]
        }
    }

    // ---------- system ----------

    def debug(enabled: Boolean = true): DataHub = {
        DEBUG = enabled
        SOURCES.values.foreach {
            case db: DataSource => db.debug(enabled)
            case excel: Excel => excel.debug(enabled)
            case _ =>
        }

        this
    }

    def debugging: Boolean = DEBUG

    def +=(dataSource:(String, Any)): DataHub = {
        SOURCES += dataSource._1 -> {
            dataSource._2 match {
                case db: DataSource => db.debug(DEBUG)
                case excel: Excel => excel.debug(DEBUG)
                case o => o
            }
        }
        this
    }

    // ---------- open ----------

    private[qross] def openSource(sourceName: String, source: Any): DataHub = {
        reset()
        if (!SOURCES.contains(sourceName) && !ALIASES.contains(sourceName.toLowerCase())) {
            this += sourceName -> source
        }
        currentSourceName = sourceName
        lastSourceName = sourceName
        this
    }

    private[qross] def saveToDestination(destinationName: String, destination: Any): DataHub = {
        if (!SOURCES.contains(destinationName) && !ALIASES.contains(destinationName.toLowerCase())) {
            this += destinationName -> destination
        }
        currentDestinationName = destinationName
        lastSourceName = destinationName
        this
    }

    def as(alias: String): DataHub = {
        if (ALIASES.contains(lastSourceName.toLowerCase())) {
            throw new DefineAliasException("Can not define alias for a alias.")
        }
        ALIASES += alias.toLowerCase() -> lastSourceName
        this
    }

    def asTable(alias: String): DataHub = {
        if (FQL.ALIASES.contains(FQL.recentTableName.toLowerCase())) {
            throw new DefineAliasException("Can not define alias for a alias.")
        }
        FQL.ALIASES += alias.toLowerCase() -> FQL.recentTableName
        this
    }

    def openCache(): DataHub = {
        openSource("CACHE", DataSource.MEMORY)
    }

    def openTemp(): DataHub = {
        openSource("TEMP", new DataSource(HOLDER))
    }

    def openDefault(): DataHub = {
        openSource("DEFAULT", DataSource.DEFAULT)
    }

    def openQross(): DataHub = {
        openSource("QROSS", DataSource.QROSS)
    }

    def open(connectionNameOrDataSource: Any): DataHub = {
        open(connectionNameOrDataSource, "")
    }

    def open(connectionNameOrDataSource: Any, database: String): DataHub = {
        connectionNameOrDataSource match {
            case connectionName: String => openSource(connectionName, new DataSource(connectionName, database))
            case dataSource: DataSource => openSource(dataSource.connectionName, dataSource)
            case _ => throw new OpenDataSourceException("Unsupported data source parameter format, only support String or DataSource")
        }
    }

    def use(databaseName: String): DataHub = {
        currentSource[DataSource].use(databaseName)
        this
    }

    // ---------- save as ----------

    def saveToCache(): DataHub = {
        saveToDestination("CACHE", DataSource.MEMORY)
    }

    def saveToTemp(): DataHub = {
        saveToDestination("TEMP", new DataSource(HOLDER))
    }

    def saveToDefault(): DataHub = {
        saveToDestination("DEFAULT", DataSource.DEFAULT)
    }

    def saveToQross(): DataHub = {
        saveToDestination("QROSS", DataSource.QROSS)
    }

    def saveTo(connectionName: String): DataHub = {
        saveTo(connectionName, "")
    }

    def saveTo(connectionName: String, database: String): DataHub = {
        saveToDestination(connectionName, new DataSource(connectionName, database))
    }

    // ---------- Reset ----------

    def reset(): DataHub = {
        if (TO_BE_CLEAR) {
            TABLE.clear()
            pageSQLs.clear()
            blockSQLs.clear()
            processSQLs.clear()
            TOTAL_COUNT_OF_RECENT_GET = 0
            TOTAL_AFFECTED_ROWS_OF_RECENT_PUT = 0
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
            this += "CACHE" -> DataSource.MEMORY
        }

        //var createSQL = "" + tableName + " (__pid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE"
        var createSQL = s"CREATE TABLE IF NOT EXISTS $tableName ("
        if (primaryKey != "" && !table.contains(primaryKey)) {
            createSQL += s" [$primaryKey] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, "
        }
        val placeHolders = new mutable.ArrayBuffer[String]()
        val fields = new mutable.ArrayBuffer[String]()
        for ((field, dataType) <- table.getColumns) {
            fields += "[" + field + "] " + dataType.toString
            placeHolders += "?"
        }
        createSQL += fields.mkString(", ") + ")"
        Source[DataSource]("CACHE").executeNonQuery(createSQL)
        fields.clear()

        if (DEBUG) {
            Output.writeDebugging(createSQL)
            Output.writeDebugging(s"${table.size} rows has been saved in cache table $tableName.")
        }

        if (table.nonEmpty) {
            Source[DataSource]("CACHE").tableUpdate("INSERT INTO " + tableName + " ([" + table.getFieldNames.mkString("], [") + "]) VALUES (" + placeHolders.mkString(",") + ")", table)
        }
        placeHolders.clear()

        TO_BE_CLEAR = true

        this
    }

    // ---------- temp ----------

    def temp(tableName: String, keys: (String, String)*): DataHub = {

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

    def temp(tableName: String, table: DataTable, keys: (String, String)*): DataHub = {

        /*
        keys format
        [PRIMARY [KEY]] keyName - if not exists in table, will add as primary key with auto increment - will be ignored if exists
        [KEY] keyName[,keyName2,...] - common index, fields must exists
        UNIQUE [KEY] keyName[,keyName2,...] - unique index, fields must exists
        */
        var primaryKey = ""
        val indexSQL = s"CREATE #{unique} INDEX IF NOT EXISTS idx_${tableName}_#{fields} ON $tableName (#{keys})"
        val indexes = new mutable.ArrayBuffer[String]()
        keys.foreach(key => {
            val index = key._1.trim().toUpperCase().replaceAll("\\s+", "$")
            val keys = key._2.trim()
            if (index.startsWith("PRIMARY")) {
                if (!index.contains(",") && !table.contains(keys)) {
                    primaryKey = keys
                }
            }
            else {
                indexes += indexSQL.replace("#{unique}", if (index.startsWith("UNIQUE")) "UNIQUE" else "")
                        .replace("#{fields}", keys.replaceAll("\\s+", "").replace(",", "_"))
                        .replace("#{keys}", keys)
            }
        })

        if (!SOURCES.contains("TEMP")) {
            this += "TEMP" -> new DataSource(HOLDER)
        }

        //var createSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (__pid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE"
        var createSQL = s"CREATE TABLE IF NOT EXISTS $tableName ("
        if (primaryKey != "") {
            createSQL += s" [$primaryKey] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, "
        }
        val placeHolders = new mutable.ArrayBuffer[String]()
        val fields = new mutable.ArrayBuffer[String]()
        for ((field, dataType) <- table.getColumns) {
            fields += "[" + field + "] " + dataType.toString
            placeHolders += "?"
        }
        createSQL += fields.mkString(", ") + ")"
        Source[DataSource]("TEMP").executeNonQuery(createSQL)
        fields.clear()

        if (DEBUG) {
            Output.writeDebugging(createSQL)
            Output.writeDebugging(s"${table.size} rows has been saved in temp table $tableName.")
        }

        if (table.nonEmpty) {
            Source[DataSource]("TEMP").tableUpdate("INSERT INTO " + tableName + " ([" + table.getFieldNames.mkString("],[") + "]) VALUES (" + placeHolders.mkString(",") + ")", table)
        }
        placeHolders.clear()

        if (indexes.nonEmpty) {
            indexes.foreach(SQL => {
                Source[DataSource]("TEMP").executeNonQuery(SQL)
            })
        }

        TO_BE_CLEAR = true

        this
    }

    // ---------- base method ----------

    //execute SQL on target dataSource
    def set(nonQuerySQL: String, values: Any*): DataHub = {
        getSource match {
            case ds: DataSource => AFFECTED_ROWS_OF_LAST_SET = ds.executeNonQuery(nonQuerySQL, values: _*)
            case excel: Excel => AFFECTED_ROWS_OF_LAST_SET = excel.executeNonQuery(nonQuerySQL)
            case None =>
        }

        this
    }

    //keyword table name must add tow type quote like "`case`" in presto
    def select(selectSQL: String, values: Any*): DataHub = get(selectSQL, values: _*)
    def get(selectSQL: String, values: Any*): DataHub = {
        //clear if new transaction
        reset()

        if (DEBUG) {
            println("GET # " + selectSQL)
        }

        getSource match {
            case ds: DataSource => TABLE.merge(ds.executeDataTable(selectSQL, values: _*))
            case excel: Excel => TABLE.merge(excel.select(selectSQL))
            case _ =>
        }

        TOTAL_COUNT_OF_RECENT_GET += TABLE.count()
        COUNT_OF_LAST_GET = TABLE.count()

        this
    }

    def join(selectSQL: String, on: (String, String)*): DataHub = {
        TABLE.join(currentSource[DataSource].executeDataTable(selectSQL), on: _*)
        this
    }

    def pass(querySentence: String, default:(String, Any)*): DataHub = {

        if (DEBUG) {
            println("PASS # " + querySentence)
        }

        if (TABLE.isEmpty) {
            if (default.nonEmpty) {
                TABLE.addRow(new DataRow(default: _*))
            }
            else {
                throw new Exception("No data to pass. Please ensure data exists or provide default value")
            }
        }

        getSource match {
            case ds: DataSource => TABLE.cut(currentSource[DataSource].tableSelect(querySentence, TABLE))
            case excel: Excel => TABLE.cut(excel.tableSelect(querySentence, TABLE))
            case _ =>
        }

        TOTAL_COUNT_OF_RECENT_GET = TABLE.count()
        COUNT_OF_LAST_GET = TABLE.count()

        this
    }

    //execute SQL on target dataSource
    def prep(nonQuerySQL: String, values: Any*): DataHub = {
        AFFECTED_ROWS_OF_LAST_PREP = getDestination match {
            case ds: DataSource =>  ds.executeNonQuery(nonQuerySQL, values: _*)
            case excel: Excel => excel.executeNonQuery(nonQuerySQL)
            case _ => 0
        }

        this
    }

    def insert(insertSQL: String): DataHub = put(insertSQL)
    def update(updateSQL: String): DataHub = put(updateSQL)
    def delete(deleteSQL: String): DataHub = put(deleteSQL)
    def put(nonQuerySQL: String): DataHub = {

        if (DEBUG) {
            println("PUT # " + nonQuerySQL)
        }

        if (TABLE.nonEmpty) {
            AFFECTED_ROWS_OF_LAST_PUT = {
                getDestination match {
                    case ds: DataSource =>  ds.tableUpdate(nonQuerySQL, TABLE)
                    case excel: Excel => excel.tableUpdate(nonQuerySQL, TABLE)
                    case _ => 0
                }
            }
            TOTAL_AFFECTED_ROWS_OF_RECENT_PUT += AFFECTED_ROWS_OF_LAST_PUT
        }
        else {
            AFFECTED_ROWS_OF_LAST_PUT = 0
        }

        if (pageSQLs.nonEmpty || blockSQLs.nonEmpty) {
            stream(table => {
                currentDestination[DataSource].tableUpdate(nonQuerySQL, table)
                table.clear()
            })
        }

        TO_BE_CLEAR = true

        this
    }

    def insert(insertSQL: String, table: DataTable): DataHub = put(insertSQL, table)
    def update(updateSQL: String, table: DataTable): DataHub = put(updateSQL, table)
    def delete(deleteSQL: String, table: DataTable): DataHub = put(deleteSQL, table)
    def put(nonQuerySQL: String, table: DataTable): DataHub = {

        if (DEBUG) {
            println("PUT # " + nonQuerySQL)
        }

        if (table.nonEmpty) {
            AFFECTED_ROWS_OF_LAST_PUT = {
                getDestination match {
                    case ds: DataSource =>  ds.tableUpdate(nonQuerySQL, table)
                    case excel: Excel => excel.tableUpdate(nonQuerySQL, table)
                    case _ => 0
                }
            }
            TOTAL_AFFECTED_ROWS_OF_RECENT_PUT += AFFECTED_ROWS_OF_LAST_PUT
        }
        else {
            AFFECTED_ROWS_OF_LAST_PUT = 0
        }

        this
    }

    // ---------- multi thread ----------

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

    //多线程, 按limit分页, 不过越往后越慢
    //select * from table limit @{offset}, 10000
    //单线程, @{id}为主键名, 必须在返回值中且与数据表字段一致, 如果不是从0开始, 在WHERE条件式中加 id>2000 条件式
    //应用场景较少, 主要解决大数据表的分页问题
    //select * from table where id>@{id} LIMIT 10000
    def page(selectSQL: String): DataHub = {
        //clear if new transaction
        reset()

        if (DEBUG) {
            println("PAGE # " + selectSQL)
        }

        selectSQL.matchBatchMark match {
            case Some(param) => pageSQLs += selectSQL -> param
            case None => TABLE.merge(currentSource[DataSource].executeDataTable(selectSQL))
        }

        this
    }

    //按主键分块读取
    //begin_id, end_id
    //每10000行一块
    //select * from table where id>@{id} AND id<=@{id}
    //select id from table order by id asc limit 1
    //select max(id) from table
    //beginKeyOrSQL为整数时左开右闭, 为SQL时左闭右闭
    def block(selectSQL: String, beginKeyOrSQL: Any, endKeyOrSQL: Any, blockSize: Int = 10000): DataHub = {

        reset()

        if (DEBUG) {
            println("BLOCK # " + selectSQL)
        }

        selectSQL.matchBatchMark match {
            case Some(param) =>
                blockSQLs += selectSQL -> (param,
                        beginKeyOrSQL match {
                            case sentence: String => currentSource[DataSource].executeDataRow(sentence).getLong(0)
                            case key: Long => key
                            case key: Int => key.toLong
                            case _ => beginKeyOrSQL.toString.toLong
                        },
                        endKeyOrSQL match {
                            case sentence: String => currentSource[DataSource].executeDataRow(sentence).getLong(0)
                            case key: Long => key
                            case key: Int => key.toLong
                            case _ => endKeyOrSQL.toString.toLong
                        },
                        blockSize)
            case None => TABLE.merge(currentSource[DataSource].executeDataTable(selectSQL))
        }

        this
    }

    //多线程执行同一条UPDATE或DELETE语句, beginKeyOrSQL为整数时左开右闭, 为SQL时左闭右闭
    //初始设计目的是解决tidb不能大批量更新的问题, 只能按块更新
    //DELETE FROM table WHERE id>@{id} AND id<=@{id}
    def bulk(nonQuerySQL: String, beginKeyOrSQL: Any, endKeyOrSQL: Any, bulkSize: Int = 100000): DataHub = {

        if (DEBUG) {
            println(nonQuerySQL)
        }

        nonQuerySQL.matchBatchMark match {
            case Some(param) =>
                var begin = beginKeyOrSQL match {
                    case sentence: String => currentSource[DataSource].executeDataRow(sentence).getLong(0) - 1
                    case key: Int => key.toLong
                    case key: Long => key
                    case _ => beginKeyOrSQL.toString.toLong
                }
                val end = endKeyOrSQL match {
                    case sentence: String => currentSource[DataSource].executeDataRow(sentence).getLong(0)
                    case key: Int => key.toLong
                    case key: Long => key
                    case _ => beginKeyOrSQL.toString.toLong
                }

                while (begin < end) {
                    Bulker.QUEUE.add(nonQuerySQL.replaceFirst(param, begin.toString).replace(param, (if (begin + bulkSize >= end) end else begin + bulkSize).toString))
                    begin += bulkSize
                }

                val parallel = new Parallel()
                //producer
                for (i <- 0 until LINES) {
                    parallel.add(new Bulker(currentSource[DataSource]))
                }
                parallel.startAll()
                parallel.waitAll()

            //blockSize
            case None => currentSource[DataSource].executeNonQuery(nonQuerySQL)
        }

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

    // #4+6 多线程生产多线程消费
    // #4+5+6 多线程加工多线程消费
    def batch(nonQuerySentence: String): DataHub = {

        if (DEBUG) {
            println(nonQuerySentence)
        }

        //pageSQLs or blockSQLs
        //processSQLs
        //batchSQL

        val parallel = new Parallel()

        //生产者
        for ((selectSQL, param) <- pageSQLs) {
            if (param.toLowerCase() == "@{offset}") {
                //offset
                val pageSize = "\\d+".r.findFirstMatchIn(selectSQL.takeAfter(param)) match {
                    case Some(ps) => ps.group(0).toInt
                    case None => 10000
                }

                //producer
                for (i <- 0 until LINES) { //Global.CORES
                    parallel.add(new Pager(currentSource[DataSource], selectSQL, param, pageSize, TANKS))
                }
            }
        }

        //生产者
        for ((selectSQL, config) <- blockSQLs) {
            //SELECT * FROM table WHERE id>@id AND id<=@id;

            var i = if (config._2 == 0) 0 else config._2 - 1
            while (i < config._3) {
                Blocker.QUEUE.add(
                    selectSQL.replaceFirst(config._1, i.toString)
                            .replaceFirst(config._1, {
                                i += config._4
                                if (i > config._3) {
                                    i = config._3
                                }
                                i.toString
                            }))
            }

            //producer
            for (i <- 0 until LINES) { //Global.CORES
                parallel.add(new Blocker(currentSource[DataSource]))
            }
        }

        //中间多个加工者
        for (index <- processSQLs.indices) {
            for (i <- 0 until LINES) {
                parallel.add(new Processer(currentSource[DataSource], processSQLs(index), index, TANKS))
            }
        }

        //最终消费者
        for (i <- 0 until LINES) {
            parallel.add(new Batcher(currentDestination[DataSource], nonQuerySentence))
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
            if (param.toLowerCase() == "@{offset}") {
                //offset
                val pageSize = "\\d+".r.findFirstIn(selectSQL.takeAfter(param)) match {
                    case Some(ps) => ps.toInt
                    case None => 10000
                }

                val parallel = new Parallel()
                //producer
                for (i <- 0 until LINES) {
                    parallel.add(new Pager(currentSource[DataSource], selectSQL, param, pageSize, TANKS))
                }
                parallel.startAll()
                //consumer
                while(Pager.DATA.size() > 0 || parallel.running) {
                    val table = Pager.DATA.poll()
                    if (table != null) {
                        TOTAL_COUNT_OF_RECENT_GET += table.count()
                        COUNT_OF_LAST_GET = table.count()
                        handler(table)
                        Output.writeMessage(s"$pageSize SAVED")
                    }
                    Timer.sleep(100)
                }
                parallel.waitAll()
                Pager.CUBE.reset()
                Output.writeMessage("Exit All Page")
            }
            else {
                //单线程, 多线程使用block方法
                //SELECT * FROM table WHERE id>@{id} LIMIT 10000;
                var id = 0L //primaryKey
                val field = param.$trim("@{", "}")
                var continue = true
                do {
                    val table = currentSource[DataSource].executeDataTable(selectSQL.replace(param, String.valueOf(id)))
                    TOTAL_COUNT_OF_RECENT_GET += table.count()
                    COUNT_OF_LAST_GET = table.count()
                    if (table.nonEmpty) {
                        if (table.contains(field)) {
                            id = table.max(field).firstCellLongValue
                        }
                        else {
                            throw new Exception("Result set must contains primary key: " + field)
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
            //SELECT * FROM table WHERE id>@id AND id<=@id;
            //producer
            var i = if (config._2 == 0) 0 else config._2 - 1
            while (i < config._3) {
                Blocker.QUEUE.add(
                    selectSQL.replaceFirst(config._1, i.toString)
                             .replaceFirst(config._1, {
                                 i += config._4
                                 if (i > config._3) {
                                     i = config._3
                                 }
                                 i.toString
                             }))
            }

            //consumer
            val parallel = new Parallel()
            //producer
            for (i <- 0 until LINES) {  //Global.CORES
                parallel.add(new Blocker(currentSource, TANKS))
            }
            parallel.startAll()
            //consumer
            while(parallel.running) {
                if (Blocker.DATA.size() > 0) {
                    val table = Blocker.DATA.poll()
                    if (table != null) {
                        TOTAL_COUNT_OF_RECENT_GET += table.count()
                        COUNT_OF_LAST_GET = table.count()
                        handler(table)
                        Output.writeMessage(s"${config._4} SAVED.")
                    }
                }
                Timer.sleep(100)
            }
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

    //逻辑同GET
    def buffer(table: DataTable): DataHub = {
        reset()

        TABLE.merge(table)
        TOTAL_COUNT_OF_RECENT_GET += TABLE.count()
        COUNT_OF_LAST_GET = TABLE.count()

        if (DEBUG) {
            TABLE.show(10)
        }

        this
    }

    def buffer(tableName: String): DataHub = {
        BUFFER += tableName -> DataTable.from(TABLE)

        TO_BE_CLEAR = true

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

    def containsBuffer(tableName: String): Boolean = {
        BUFFER.contains(tableName)
    }

    def takeOut(): DataTable = {
        val table = DataTable.from(TABLE)
        TABLE.clear()
        table
    }

    def takeOut(tableName: String): DataTable = {
        if (BUFFER.contains(tableName)) {
            val table = DataTable.from(BUFFER(tableName))
            BUFFER.remove(tableName)
            table
        }
        else {
            new DataTable()
        }
    }

    def getData: DataTable = {
        TABLE
    }

    def getData(tableName: String): DataTable = {
        if (BUFFER.contains(tableName)) {
            BUFFER(tableName)
        }
        else {
            new DataTable()
        }
    }

    def preclear(): DataHub = {
        TO_BE_CLEAR = true
        this
    }

    def par: ParArray[DataRow] = {
        TABLE.par
    }

    def firstRow: DataRow = {
        TABLE.firstRow match {
            case Some(row) => row
            case None => new DataRow()
        }
    }

    def lastRow: DataRow = {
        TABLE.lastRow match {
            case Some(row) => row
            case None => new DataRow()
        }
    }

    def getRow(i: Int): DataRow = {
        TABLE.getRow(i) match {
            case Some(row) => row
            case None => new DataRow()
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

    def label(alias: String): DataHub = {
        TABLE.label(alias.split(","))
        this
    }

    def label(alias: Array[String]): DataHub = {
        TABLE.label(alias)
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

        TABLE.insert(fields: _*)
        this
    }

    def insertRowIfEmpty(fields: (String, Any)*): DataHub = {

        if (TO_BE_CLEAR) {
            TABLE.clear()
            TO_BE_CLEAR = false
        }

        if (TABLE.isEmpty) {
            TABLE.insert(fields: _*)
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


    // ---------- Extends ----------

    def plug(func: String, extend: Any): DataHub = {
        SLOTS += func -> extend
        this
    }

    def slots(func: String): Boolean = {
        SLOTS.contains(func)
    }

    def pull(func: String): DataHub = {
        SLOTS -= func
        this
    }

    def pick[T](func: String): Option[T] = {
        if (SLOTS.contains(func)) {
            Some(SLOTS(func).asInstanceOf[T])
        }
        else {
            None
        }
    }

    // ---------- DataSource ----------

    def executeDataTable(SQL: String, values: Any*): DataTable = {
        if (Patterns.$SELECT$CUSTOM.test(SQL)) {
            FQL.select(SQL, values: _*)
        }
        else {
            getSource match {
                case ds: DataSource => ds.executeDataTable(SQL, values: _*)
                case excel: Excel => excel.select(SQL, values: _*)
                case _ => new DataTable()
            }
        }
    }
    def executeDataRow(SQL: String, values: Any*): DataRow = currentSource[DataSource].executeDataRow(SQL, values: _*)
    def executeJavaMap(SQL: String, values: Any*): java.util.Map[String, Any] = currentSource[DataSource].executeJavaMap(SQL, values: _*)
    def executeJavaMapList(SQL: String, values: Any*): java.util.List[java.util.Map[String, Any]] = currentSource[DataSource].executeJavaMapList(SQL, values: _*)
    def executeJavaList(SQL: String, values: Any*): java.util.List[Any] = currentSource[DataSource].executeJavaList(SQL, values: _*)
    def executeHashMap(SQL: String, values: Any*): Map[String, Any] = currentSource[DataSource].executeHashMap(SQL, values: _*)
    def executeMapList(SQL: String, values: Any*): List[Map[String, Any]] = currentSource[DataSource].executeMapList(SQL, values: _*)
    def executeSingleList[T](SQL: String, values: Any*): List[Any] = currentSource[DataSource].executeSingleList[T](SQL, values: _*)
    def executeSingleValue(SQL: String, values: Any*): DataCell = currentSource[DataSource].executeSingleValue(SQL, values: _*)
    def executeExists(SQL: String, values: Any*): Boolean = currentSource[DataSource].executeExists(SQL, values: _*)
    def executeNonQuery(SQL: String, values: Any*): Int = currentSource[DataSource].executeNonQuery(SQL, values: _*)

    def close(): Unit = {
        SOURCES.values.foreach {
            case db: DataSource => db.close()
            case excel: Excel => excel.close()
            case _ =>
        }

        FQL.close()

        SOURCES.clear()
        BUFFER.clear()
        TABLE.clear()

        SLOTS.clear()
        HOLDER.delete()
    }
}