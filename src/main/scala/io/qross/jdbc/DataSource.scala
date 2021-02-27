package io.qross.jdbc

import java.sql._
import java.util
import java.util.regex.Pattern

import io.qross.core.Parameter._
import io.qross.core._
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.time.Timer

import scala.collection.mutable

object DataSource {

    def QROSS: DataSource = new DataSource(JDBC.QROSS)

    def DEFAULT : DataSource = new DataSource(JDBC.DEFAULT)

    def MEMORY: DataSource = new DataSource(DBType.Memory)

}

class DataSource (val connectionName: String, val databaseName: String) extends Output {

    private[jdbc] val batchSQLs = new mutable.ArrayBuffer[String]()
    private[jdbc] val batchValues = new mutable.ArrayBuffer[Vector[Any]]()

    private[jdbc] var config = JDBC.get(connectionName)

    //是否启用调试
    private var DEBUG = false

    private var connection: Option[Connection] = None //current connection
    private var tick: Long = -1L //not opened

    def this() {
        this(connectionName = JDBC.DEFAULT, databaseName = "")
    }

    def this(connectionName: String) {
        this(connectionName, databaseName = "")
    }
    
    def testConnection(): Boolean = {
        var connected = false
        try {
            this.executeResultSet("SELECT 1 AS test")
            connected = true
        }
        catch {
            case _: Exception =>
        }

        connected
    }

    def debug(enabled: Boolean = true, format: String = "text"): DataSource = {
        DEBUG = enabled
        LOG_FORMAT = format
        this
    }
    //是否启用调试模式
    def debugging: Boolean = DEBUG
    
    def open(): Unit = {
        //Class.forName(config.driver).newInstance()
        //检查driver
        try {
            Class.forName(config.driver).newInstance()
        }
        catch {
            case e: ClassNotFoundException =>
                if (config.alternativeDriver != "") {
                    //检查备选driver
                    try {
                        Class.forName(config.alternativeDriver).newInstance()
                    }
                    catch {
                        case e: ClassNotFoundException => System.err.println("Open database ClassNotFoundException: " + e.getMessage)
                    }
                }
                else {
                    System.err.println("Open database ClassNotFoundException: " + e.getMessage)
                }
            case e: InstantiationException => System.err.println("Open database InstantiationException: " + e.getMessage)
            case e: IllegalAccessException => System.err.println("Open database IllegalAccessException: " + e.getMessage)
        }

        if (config.username != "") {
            this.connection = Some(DriverManager.getConnection(config.connectionString, config.username, config.password))
        }
        else {
            this.connection = Some(DriverManager.getConnection(config.connectionString))
        }

        //尝试连接
//        try {
//            if (config.username != "") {
//                this.connection = Some(DriverManager.getConnection(config.connectionString, config.username, config.password))
//            }
//            else {
//                this.connection = Some(DriverManager.getConnection(config.connectionString))
//            }
//        } catch {
//            case e: SQLException => System.err.println("Open database SQLException: " + e.getMessage)
//        }

        if (config.dbType == DBType.MySQL) {
            this.connection match {
                case Some(conn) =>
                    try {
                        val prest: PreparedStatement = conn.prepareStatement("SELECT 1 AS T")
                        val rs = prest.executeQuery()
                        rs.close()
                        prest.close()
                    }
                    catch {
                        case e: Exception =>
                            e.printStackTrace()
                            System.err.println("Test connection Exception: " + e.getMessage)
                            connection = None
                    }
                case None =>
            }
        }

        if (this.databaseName != "") {
            this.use(this.databaseName)
        }
    }

    def use(databaseName: String): Unit = {
        this.connection match {
            case Some(conn) =>
                val prest: PreparedStatement = conn.prepareStatement("USE " + databaseName)
                prest.executeUpdate()
                prest.close()
            case None =>
        }
    }
    
    // ---------- basic command ----------

    def executeDataTable(SQL: String, values: Any*): DataTable = {

        if (DEBUG) {
            writeCode(SQL)
        }

        val table: DataTable = new DataTable()
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                val meta: ResultSetMetaData = rs.getMetaData
                val columns: Int = meta.getColumnCount
                //var fieldName = ""
                for (i <- 1 to columns) {
                    //fieldName = meta.getColumnLabel(i) //meta.getColumnName(i) original name
                    // '.' is illegal char in SQLite and field name often contains "." in hive columns
                    //if (fieldName.contains(".")) fieldName = fieldName.substring(fieldName.lastIndexOf(".") + 1)
                    //if (!Pattern.matches("^[a-zA-Z_][a-zA-Z0-9_]*$", fieldName) || table.contains(fieldName)) fieldName = "column" + i
                    //println(meta.getColumnLabel(i) + ": " + meta.getColumnTypeName(i) + ", " + meta.getColumnClassName(i))
                    table.addField(meta.getColumnLabel(i), DataType.ofTypeName(meta.getColumnTypeName(i), meta.getColumnClassName(i)))
                }

                val fields = table.getFieldNames
                while (rs.next) {
                    val row = table.newRow()
                    for (i <- 1 to columns) {
                        val dataType = table.getFieldDataType(fields(i-1))
                        row.set(fields(i-1), {
                            val value = rs.getObject(i)
                            if (dataType == DataType.INTEGER) {
                                if (value != null) {
                                    if (dataType.className == "java.math.BigInteger") {
                                        value.asInstanceOf[java.math.BigInteger]
                                    }
                                    else if (dataType.originalName == "BIGINT" || dataType.originalName == "LONG") {
                                        value.toInteger(0)
                                    }
                                    else {
                                        rs.getInt(i)
                                    }
                                }
                                else {
                                    value
                                }
                            }
                            else if (dataType == DataType.DECIMAL) {
                                if (value != null) {
                                    if (dataType.className == "java.math.BigDecimal") {
                                        value.asInstanceOf[java.math.BigDecimal].stripTrailingZeros()
                                    }
                                    else if (dataType.originalName == "DOUBLE" || dataType.originalName == "DECIMAL") {
                                        value.toDecimal(0)// .asInstanceOf[Double]
                                    }
                                    else {
                                        rs.getFloat(i)
                                        //value.asInstanceOf[Float]
                                    }
                                }
                                else {
                                    value
                                }
                            }
                            else {
                                value
                            }
                        }, dataType)
                    }
                    table.insert(row)
                }
                if (config.dbType != DBType.Presto) {
                    rs.getStatement.close()
                }
                rs.close()

            case None =>
        }

        if (DEBUG) {
            writeTable(table, 10)
        }
        
        table
    }

    def executeJavaMapList(SQL: String, values: Any*): util.List[util.Map[String, Any]] = {

        if (DEBUG) {
            writeCode(SQL)
        }

        val mapList: util.List[util.Map[String, Any]] = new util.ArrayList[util.Map[String, Any]]()

        this.executeResultSet(SQL, values: _*) match {
            case Some (rs) =>
                val meta = rs.getMetaData
                val columns: Int = meta.getColumnCount
                while (rs.next) {
                    val row: util.Map[String, Any] = new util.HashMap[String, Any]()
                    for (i <- 1 to columns) {
                        row.put(meta.getColumnLabel(i), rs.getObject(i))
                    }
                    mapList.add(row)
                }
                if (config.dbType != DBType.Presto) {
                    rs.getStatement.close()
                }
                rs.close()
            case None =>
        }

        if (DEBUG) {
            writeCode(Json.serialize(mapList), "json")
        }

        mapList
    }

    def executeMapList(SQL: String, values: Any*): List[Map[String, Any]] = {

        if (DEBUG) {
            println()
            println(SQL)
            println("------------------------------------------------------------------------")
        }

        val mapList: mutable.ListBuffer[Map[String, Any]] = new mutable.ListBuffer[Map[String, Any]]()

        this.executeResultSet(SQL, values: _*) match {
            case Some (rs) =>
                val meta = rs.getMetaData
                val columns: Int = meta.getColumnCount
                while (rs.next) {
                    val row: mutable.HashMap[String, Any] = new mutable.HashMap[String, Any]()
                    for (i <- 1 to columns) {
                        row.put(meta.getColumnLabel(i), rs.getObject(i))
                    }
                    mapList += row.toMap
                }
                if (config.dbType != DBType.Presto) {
                    rs.getStatement.close()
                }
                rs.close()
            case None =>
        }

        if (DEBUG) {
            println(Json.serialize(mapList))
        }

        mapList.toList
    }

    def executeDataRow(SQL: String, values: Any*): DataRow = {

        if (DEBUG) {
            println()
            println(SQL)
            println("------------------------------------------------------------------------")
        }

        val row: DataRow = new DataRow
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                val meta: ResultSetMetaData = rs.getMetaData
                val columns: Int = rs.getMetaData.getColumnCount
                if (rs.next) {
                    for (i <- 1 to columns) {
                        row.set(meta.getColumnLabel(i), rs.getObject(i))
                    }
                    if (config.dbType != DBType.Presto) {
                        rs.getStatement.close()
                    }
                    rs.close()
                }
            case None =>
        }

        if (DEBUG) {
            println(row.getFields.mkString(", "))
            println("------------------------------------------------------------------------")
            println(row.mkString(", "))
        }

        row
    }

    def executeJavaMap(SQL: String, values: Any*): util.Map[String, Any] = {

        if (DEBUG) {
            println()
            println(SQL)
            println("------------------------------------------------------------------------")
        }

        val map: util.Map[String, Any] = new util.HashMap[String, Any]()
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                val meta: ResultSetMetaData = rs.getMetaData
                val columns: Int = rs.getMetaData.getColumnCount
                if (rs.next) {
                    for (i <- 1 to columns) {
                        map.put(meta.getColumnLabel(i), rs.getObject(i))
                    }
                    if (config.dbType != DBType.Presto) {
                        rs.getStatement.close()
                    }
                    rs.close()
                }
            case None =>
        }

        if (DEBUG) {
            println(Json.serialize(map))
        }

        map
    }

    def executeHashMap(SQL: String, values: Any*): Map[String, Any] = {

        if (DEBUG) {
            println()
            println(SQL)
            println("------------------------------------------------------------------------")
        }

        val map: mutable.HashMap[String, Any] = new mutable.HashMap[String, Any]()
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                val meta: ResultSetMetaData = rs.getMetaData
                val columns: Int = rs.getMetaData.getColumnCount
                if (rs.next) {
                    for (i <- 1 to columns) {
                        map.put(meta.getColumnLabel(i), rs.getObject(i))
                    }
                    if (config.dbType != DBType.Presto) {
                        rs.getStatement.close()
                    }
                    rs.close()
                }
            case None =>
        }

        if (DEBUG) {
            println(Json.serialize(map))
        }

        map.toMap
    }

    def executeDataMap[S, T](SQL: String, values: Any*): Map[S, T] = {

        if (DEBUG) {
            println()
            println(SQL)
            println("------------------------------------------------------------------------")
        }

        val map: mutable.HashMap[S, T] = new mutable.HashMap[S, T]()
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                while (rs.next) {
                    map.put(rs.getObject(1).asInstanceOf[S], rs.getObject(2).asInstanceOf[T])
                }
                if (config.dbType != DBType.Presto) {
                    rs.getStatement.close()
                }
                rs.close()
            case None =>
        }

        if (DEBUG) {
            println(Json.serialize(map))
        }

        map.toMap
    }
    
    def executeJavaList(SQL: String, values: Any*): util.List[Any] = {

        if (DEBUG) {
            println()
            println(SQL)
            println("------------------------------------------------------------------------")
        }

        val list: util.List[Any] = new util.ArrayList[Any]()
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                while (rs.next) {
                    list.add(rs.getObject(1))
                }
                if (config.dbType != DBType.Presto) {
                    rs.getStatement.close()
                }
                rs.close()
            case None =>
        }

        if (DEBUG) {
            println(Json.serialize(list))
        }

        list
    }

    def executeSingleList[T](SQL: String, values: Any*): List[T] = {

        if (DEBUG) {
            println()
            println(SQL)
            println("------------------------------------------------------------------------")
        }

        val list: mutable.ListBuffer[T] = new mutable.ListBuffer[T]()
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                while (rs.next) {
                    list += rs.getObject(1).asInstanceOf[T]
                }
                if (config.dbType != DBType.Presto) {
                    rs.getStatement.close()
                }
                rs.close()
            case None =>
        }

        if (DEBUG) {
            println(Json.serialize(list))
        }

        list.toList
    }
    
    def executeSingleValue(SQL: String, values: Any*): DataCell = {

        if (DEBUG) {
            println()
            println(SQL)
            println("------------------------------------------------------------------------")
        }

        var data: DataCell = DataCell.UNDEFINED
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                if (rs.next()) {
                    data = DataCell(rs.getObject(1), DataType.ofTypeName(rs.getMetaData.getColumnTypeName(1), rs.getMetaData.getColumnClassName(1)))
                    if (config.dbType != DBType.Presto) {
                        rs.getStatement.close()
                    }
                    rs.close()
                }
            case None =>
        }

        if (DEBUG) {
            println("Result: " + data)
        }

        data
    }
    
    def executeExists(SQL: String, values: Any*): Boolean = {

        if (DEBUG) {
            println()
            println(SQL)
            println("------------------------------------------------------------------------")
        }

        var result = false
        this.executeResultSet(SQL, values: _*) match {
            case Some(rs) =>
                if (rs.next()) {
                    result = true
                    if (config.dbType != DBType.Presto) {
                        rs.getStatement.close()
                    }
                    rs.close()
                }
            case None =>
        }

        if (DEBUG) {
            println("Result: " + result)
        }
        
        result
    }
    
    def executeResultSet(SQL: String, values: Any*): Option[ResultSet] = {
        this.openIfNot()
        
        this.connection match {
            case Some(conn) =>
                var rs: Option[ResultSet] = None
                var retry: Int = 0
                while (rs.isEmpty && retry < 3) {
                    //try {
                        val prest: PreparedStatement = conn.prepareStatement(trimSQL(SQL))
                        for (i <- 0 until values.length) {
                            prest.setObject(i + 1, values(i))
                        }
                        rs = Some(prest.executeQuery)
                        //prest.close()
                    //} catch {
                    //    case e: SQLException => e.printStackTrace()
                            //if (e.getClass.getSimpleName == "CommunicationsException") {
                            //    Console.writeMessage("MATCHED!")
                            //}
                    //}
                    retry += 1
                }
                rs
            case None => None
        }
    }

    def executeNonQuery(SQL: String, values: Any*): Int = {
        this.openIfNot()

        if (DEBUG) {
            writeCode(SQL)
        }

        var row: Int = -1
        this.connection match {
            case Some(conn) =>

                var retry: Int = 0
                while(row == -1 && retry < 3) {
                    //try {
                        val prest: PreparedStatement = conn.prepareStatement(trimSQL(SQL))
                        for (i <- 0 until values.length) {
                            prest.setObject(i + 1, values(i))
                        }
                        row = prest.executeUpdate

                        prest.close()
                    //} catch {
                    //    case e: SQLException => e.printStackTrace()
                    //}
                    retry += 1
                }
            case None =>
        }

        if (DEBUG) {
            writeAffected(row)
        }

        row
    }

    // ---------- batch update ----------

    def addBatchCommand(SQL: String): Unit = {
        this.batchSQLs += trimSQL(SQL)
    }

    def executeBatchCommands(): Int = {
        this.openIfNot()

        this.connection match {
            case Some(conn) =>
                var count = 0
                if (this.batchSQLs.nonEmpty) {
                    //try {
                        conn.setAutoCommit(false)
                        val stmt = conn.createStatement()
                        //val prest: PreparedStatement = conn.prepareStatement("")
                        this.batchSQLs.foreach(SQL => {
                            stmt.addBatch(SQL)
                            count += 1

                            if (DEBUG && count <= 10) {
                                println(SQL)
                            }
                        })
                        stmt.executeBatch()
                        conn.commit()
                        conn.setAutoCommit(true)
                        stmt.clearBatch()
                        stmt.close()

                    //}
                    //catch {
                    //    case e: SQLException => e.printStackTrace()
                    //}
                    this.batchSQLs.clear()
                }

                count
            case None => 0
        }
    }

    def setBatchCommand(SQL: String): Unit = {
        if (this.batchSQLs.nonEmpty) {
            this.batchSQLs.clear()
        }
        this.batchSQLs += trimSQL(SQL)
    }

    def addBatch(values: Any*): Unit = {
        this.batchValues += values.toVector
    }

    def addBatch(values: List[Any]): Unit = {
        this.batchValues += values.toVector
    }

    def addBatch(values: Vector[Any]): Unit = {
        this.batchValues += values
    }

    def addBatch(values: java.util.List[Any]): Unit = {
        this.batchValues += values.toArray.toVector
    }

    def executeBatchUpdate(commitOnExecute: Boolean = true): Int = {
        this.openIfNot()

        this.connection match {
            case Some(conn) =>
                var count: Int = 0
                if (this.batchSQLs.nonEmpty) {
                    if (this.batchValues.nonEmpty) {

                        if (DEBUG) {
                            writeCode(this.batchSQLs(0))
                        }

                        conn.setAutoCommit(false)
                        val prest: PreparedStatement = conn.prepareStatement(this.batchSQLs(0))
                        for (values <- this.batchValues) {
                            for (i <- values.indices) {
                                prest.setObject(i + 1, values(i))
                            }
                            prest.addBatch()

                            count += 1
                            if (count % 1000 == 0) {
                                prest.executeBatch
                                if (commitOnExecute) {
                                    conn.commit()
                                }
                            }
                        }
                        if (count % 1000 > 0) {
                            prest.executeBatch
                        }
                        conn.commit()
                        conn.setAutoCommit(true)
                        prest.clearBatch()
                        prest.close()

                        this.batchValues.clear()
                    }
                    this.batchSQLs.clear()
                }

                count

            case None => 0
        }
    }

    //batchSQL 可以是完整的INSERT语句，但是会自动把VALUES后面的内容去掉
    //也可以是 VALUES 前面的内容
    //也可以包含 VALUES
    def executeBatchInsert(batchSize: Int = 1000): Int = {
        var count: Int = -1
        if (this.batchSQLs.nonEmpty && this.batchValues.nonEmpty) {

            var SQL = this.batchSQLs(0)

            //去掉VALUES后面的内容
            """(?i)\)\s*VALUES\s*\(""".r.findFirstIn(SQL) match {
                case Some(values) => SQL = SQL.takeBefore(values) + ") VALUES "
                case None =>
            }

            //自动增加VALUES
            """(?i)\)\s*VALUES\s*$""".r.findFirstIn(SQL) match {
                case Some(_) =>
                case None => SQL += " VALUES "
            }

            //好像只适用于MySQL, 无论什么类型, 都加单引号
            var rows = new mutable.ArrayBuffer[String]
            var v: String = ""
            var vs: String = ""
            for (values <- this.batchValues) {
                vs = "('"
                for (i <- values.indices) {
                    v = values(i).toString
                    if (i > 0) {
                        vs += "', '"
                    }
                    vs += v.preventInjection
                }
                vs += "')"
                rows += vs

                if (rows.size >= batchSize) {
                    count += this.executeNonQuery(SQL + rows.mkString(","))
                    rows.clear()
                }
            }
            if (rows.nonEmpty) {
                count += this.executeNonQuery(SQL + rows.mkString(",").replace("~u0027", "'"))
                rows.clear()
            }
            this.batchValues.clear()
            this.batchSQLs.clear()
        }

        count
    }

    def tableSelect(SQL: String, table: DataTable): DataTable = {
        val result = new DataTable()

        if (SQL.hasQuestionMark) {
            table.par.foreach(row => {
                result.merge(this.executeDataTable(SQL, row.getValues: _*))
            })
        }
        else {
            val params = SQL.pickParameters()
            if (params.nonEmpty) {
                table.par.foreach(row => {
                    result.merge(this.executeDataTable(SQL.replaceParameters(params, row).replace("~u0027", "'")))
                })
            }
            else {
                result.merge(this.executeDataTable(SQL))
            }
        }

        result
    }

    //有?占位符
    //有#或&占位符
    //没有占位符，但是INSERT

    def tableUpdate(SQL: String, table: DataTable): Int = {

        if (DEBUG) {
            writeCode(SQL)
        }

        var count = -1
        if (table.nonEmpty) {
            SQL.placeHolderType match {
                case Parameter.MARK =>
                    this.setBatchCommand(SQL)
                    table.foreach(row => {
                        this.addBatch(row.getValues)
                    })
                    count = this.executeBatchUpdate()
                case Parameter.SHARP =>
                    val params = SQL.pickParameters()
                    table.foreach(row => {
                        this.addBatchCommand(SQL.replaceParameters(params, row).replace("~u0027", "'"))
                    })
                    count = this.executeBatchCommands()
                case Parameter.NONE =>
                    //MySQL INSERT的串接模式, 要求SQL语句不能写VALUES后面的内容
                    if ("""(?i)^(INSERT|REPLACE)\b""".r.test(SQL.trim()) && !"""(?i)\)\s*VALUES\s*\(""".r.test(SQL)) {

                        val VALUES = {
                            if (!"""(?i)\)\s*VALUES\s*$""".r.test(SQL)) {
                                " VALUES "
                            }
                            else {
                                ""
                            }
                        }
                        val values = new mutable.ArrayBuffer[String]()
                        table.foreach(row => {
                            val vs = new mutable.ArrayBuffer[String]()
                            for ((field, dataType) <- row.columns) {
                                val v = row.getString(field)
                                if (v == null) {
                                    vs += "null"
                                }
                                else if (dataType == DataType.INTEGER || dataType == DataType.DECIMAL || dataType == DataType.BOOLEAN) {
                                    vs += v
                                }
                                else {
                                    vs += "'" + v.preventInjection + "'"
                                }
                            }
                            values += "(" + vs.mkString(",") + ")"

                            vs.clear()
                        })

                        count = this.executeNonQuery(SQL + VALUES + values.mkString(",").replace("~u0027", "'"))
                        values.clear()
                    }
                    else {
                        count = this.executeNonQuery(SQL)
                    }
            }
        }

        count
    }

    def tableInsert(SQL: String, table: DataTable): Long = {
        tableUpdate(SQL, table)
    }

    def tableDelete(SQL: String, table: DataTable): Long = {
        tableUpdate(SQL, table)
    }

    // ----------- Disposable -----------

    def queryDataTable(SQL: String, values: Any*): DataTable = {
        val dataTable: DataTable = this.executeDataTable(SQL, values: _*)
        this.close()
        dataTable
    }

    def queryDataRow(SQL: String, values: Any*): DataRow = {
        val dataRow: DataRow = this.executeDataRow(SQL, values: _*)
        this.close()
        dataRow
    }

    def queryMapList(SQL: String, values: Any*): List[Map[String, Any]] = {
        val mapList = this.executeMapList(SQL, values: _*)
        this.close()
        mapList
    }

    def queryHashMap(SQL: String, values: Any*): Map[String, Any] = {
        val map = this.executeHashMap(SQL, values: _*)
        this.close()
        map
    }

    def queryDataMap[S, T](SQL: String, values: Any*): Map[S, T] = {
        val map = this.executeDataMap[S, T](SQL, values: _*)
        this.close()
        map
    }

    def querySingleList[T](SQL: String, values: Any*): List[T] = {
        val value: List[T] = this.executeSingleList[T](SQL, values: _*)
        this.close()
        value
    }

    def querySingleValue(SQL: String, values: Any*): DataCell = {
        val value: DataCell = this.executeSingleValue(SQL, values: _*)
        this.close()
        value
    }

    def queryUpdate(SQL: String, values: Any*): Int = {
        val rows: Int = this.executeNonQuery(SQL, values: _*)
        this.close()
        rows
    }

    def queryExists(SQL: String, values: Any*): Boolean = {
        val exists = this.executeExists(SQL, values: _*)
        this.close()
        exists
    }

    def queryTest(): Boolean = {
        this.open()
        val connected = this.connection != null
        this.close()
        connected
    }

    // ---------- storeproceure ----------

    // {call storedprocedure(?,?)}
    def processUpdate(storedProcedure: String, values: Any*): Int = {
        var row: Int = -1
        //try {
            val calst: CallableStatement = this.connection.get.prepareCall("{call " + storedProcedure + "}")
            for (i <- 0 until values.length) {
                calst.setObject(i + 1, values(i))
            }
            row = calst.executeUpdate
            calst.close()
        //} catch {
            //case e: SQLException => e.printStackTrace()
        //}

        row
    }

    def processResult(storedProcedure: String, values: Any*): Unit = {

    }

    // --------- other ----------

    def getIdleTime: Long = {
        this.tick match {
            case -1 => -1L
            case _ => System.currentTimeMillis - this.tick
        }
    }

    def openIfNot(): Unit = {
        try {
            var retry = 0
            //idle 10s
            if (config.overtime > 0 && this.getIdleTime >= config.overtime) {
                this.close()
            }
            if (this.tick == -1 || this.connection.get.isClosed) {
                while (this.connection.isEmpty && (config.retryLimit == 0 || retry < config.retryLimit)) {
                    this.open()
                    if (this.connection.isEmpty) {
                        Timer.sleep(1000)
                        retry += 1
                    }
                }

                if (this.connection.isDefined) {
                    this.tick = System.currentTimeMillis
                }
            }
        } catch {
            case e: SQLException => e.printStackTrace()
        }
    }
    
    def close(): Unit = {
        try {
            if (this.connection.isDefined && !this.connection.get.isClosed) {
                this.connection.get.close()
                this.connection = None
            }
        } catch {
            case e: SQLException => "close database Exception: " + e.printStackTrace()
        }
        this.tick = -1
    }
    
    private def trimSQL(SQL: String): String = {
        var commandText = SQL.trim

        if ("""(?i)^[a-z]+\s*#""".r.test(commandText)) {
            commandText = commandText.takeAfter("#")
        }

        if (commandText.endsWith(";")) {
            commandText = commandText.dropRight(1)
        }
        
        commandText
    }
}