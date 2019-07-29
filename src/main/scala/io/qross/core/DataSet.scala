package io.qross.ext

import io.qross.core.{DataCell, DataRow, DataTable}
import io.qross.jdbc.DataSource
import io.qross.core.DataType.DataType
import io.qross.net.Json

import scala.collection.mutable

// DataSet = DataHub Lite

class DataSet(defaultSourceName: String = DataSource.DEFAULT) {

    private val SOURCES = mutable.HashMap[String, DataSource](
        "DEFAULT" -> DataSource.openDefault()
    )

    private var CURRENT = SOURCES("DEFAULT")   //current dataSource - open
    private var TARGET = SOURCES("DEFAULT")    //current dataDestination - saveAs

    val TABLE = DataTable()  //current buffer
    val BUFFER = new mutable.HashMap[String, DataTable]() //all buffer

    private var JSON: Json = _
    private var TO_BE_CLEAR: Boolean = false


    // ---------- open ----------

    def openCache(): DataSet = {
        reset()
        if (!SOURCES.contains("CACHE")) {
            SOURCES += "CACHE" -> DataSource.createMemoryDatabase
        }
        CURRENT = SOURCES("CACHE")
        this
    }

    def openDefault(): DataSet = {
        CURRENT = SOURCES("DEFAULT")
        this
    }

    def open(connectionName: String): DataSet = {
        if (!SOURCES.contains(connectionName)) {
            SOURCES += connectionName -> new DataSource(connectionName)
        }
        CURRENT = SOURCES(connectionName)
        this
    }

    // ---------- save as ----------

    def saveAsCache(): DataSet = {
        if (!SOURCES.contains("CACHE")) {
            SOURCES += "CACHE" -> DataSource.createMemoryDatabase
        }
        TARGET = SOURCES("CACHE")
        this
    }

    def saveAsDefault(): DataSet = {
        TARGET = SOURCES("DEFAULT")
        this
    }

    def saveAs(connectionName: String): DataSet = {
        if (!SOURCES.contains(connectionName)) {
            SOURCES += connectionName -> new DataSource(connectionName)
        }
        TARGET = SOURCES(connectionName)
        this
    }

    // ---------- Reset ----------

    def reset(): DataSet = {
        if (TO_BE_CLEAR) {
            TABLE.clear()
            TO_BE_CLEAR = false
        }
        this
    }

    // ---------- cache ----------

    def cache(tableName: String): DataSet = {
        if (TABLE.nonEmptySchema) {
            this.cache(tableName, TABLE)
        }
        this
    }

    def cache(tableName: String, table: DataTable): DataSet = {

        if (!SOURCES.contains("CACHE")) {
            SOURCES += "CACHE" -> DataSource.createMemoryDatabase
        }

        //var createSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (__pid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE"
        var createSQL = ""
        val placeHolders = new mutable.ArrayBuffer[String]
        for ((field, dataType) <- table.getFields) {
            if (createSQL.nonEmpty) {
                createSQL += ", "
            }
            createSQL += field + " " + dataType.toString
            placeHolders += "?"
        }
        createSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + createSQL + ");"

        SOURCES("CACHE").executeNonQuery(createSQL)

        if (table.nonEmpty) {
            SOURCES("CACHE").tableUpdate("INSERT INTO " + tableName + " (" + table.getFieldNames.mkString(",") + ") VALUES (" + placeHolders.mkString(",") + ")", table)
        }
        placeHolders.clear()

        TO_BE_CLEAR = true

        this
    }

    // ---------- base method ----------

    def get(selectSQL: String, values: Any*): DataSet = {
        if (TO_BE_CLEAR) {
            TABLE.clear()
            TO_BE_CLEAR = false
        }

        TABLE.merge(CURRENT.executeDataTable(selectSQL, values: _*))
        this
    }

    //execute non-query SQL on source dataSource
    def set(nonQuerySQL: String, values: Any*): DataSet = {
        CURRENT.executeNonQuery(nonQuerySQL, values: _*)
        this
    }

    def join(selectSQL: String, on: (String, String)*): DataSet = {
        TABLE.join(CURRENT.executeDataTable(selectSQL), on: _*)
        this
    }

    //execute non-query SQL on target dataSource
    def fit(nonQuerySQL: String, values: Any*): DataSet = {
        TARGET.executeNonQuery(nonQuerySQL, values: _*)
        this
    }

    def put(nonQuerySentence: String): DataSet = {
        TARGET.tableUpdate(nonQuerySentence, TABLE)
        TO_BE_CLEAR = true
        this
    }

    def put(nonQuerySentence: String, table: DataTable): DataSet = {
        TARGET.tableUpdate(nonQuerySentence, table)
        this
    }

    // ---------- buffer basic ----------

    //switch table
    def from(tableName: String): DataSet = {
        TABLE.clear()

        if (BUFFER.contains(tableName)) {
            TABLE.union(BUFFER(tableName))
        }
        else {
            throw new Exception(s"There is no table named $tableName in buffer.")
        }
        this
    }

    def buffer(tableName: String, table: DataTable): DataSet = {
        BUFFER += tableName -> table
        TABLE.copy(table)
        this
    }

    def buffer(table: DataTable): DataSet = {
        TABLE.copy(table)
        this
    }

    def buffer(tableName: String): DataSet = {
        BUFFER += tableName -> DataTable.from(TABLE)
        TO_BE_CLEAR = true
        this
    }

    def merge(table: DataTable): DataSet = {
        TABLE.merge(table)
        this
    }

    def merge(tableName: String, table: DataTable): DataSet = {
        if (BUFFER.contains(tableName)) {
            BUFFER(tableName).merge(table)
        }
        else {
            BUFFER += tableName -> table
        }
        this
    }

    def union(table: DataTable): DataSet = {
        TABLE.union(table)
        this
    }

    def union(tableName: String, table: DataTable): DataSet = {
        if (BUFFER.contains(tableName)) {
            BUFFER(tableName).union(table)
        }
        else {
            BUFFER += tableName -> table
        }
        this
    }

    def takeOut(): DataTable = {
        DataTable.from(TABLE)
    }

    def takeOut(tableName: String): DataTable = {
        if (BUFFER.contains(tableName)) {
            BUFFER(tableName)
        }
        else {
            DataTable()
        }
    }

    def firstRow: DataRow = {
        TABLE.firstRow match {
            case Some(row) => row
            case None => DataRow()
        }
    }

    def clear(): DataSet = {
        TABLE.clear()
        this
    }

    def discard(tableName: String): DataSet = {
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

    def show(limit: Int = 10): DataSet = {
        TABLE.show(limit)
        this
    }

    // ---------- buffer action ----------

    def label(alias: (String, String)*): DataSet = {
        TABLE.label(alias: _*)
        this
    }

    def pass(querySentence: String, default:(String, Any)*): DataSet = {
        if (TABLE.isEmpty) {
            if (default.nonEmpty) {
                TABLE.addRow(DataRow(default: _*))
            }
//            else {
//                throw new Exception("No data to pass. Please ensure data exists or default value provided.")
//            }
        }
        else {
            TABLE.cut(CURRENT.tableSelect(querySentence, TABLE))
        }

        this
    }

    def getColumn(fieldName: String): List[Any] = {
        TABLE.getColumn(fieldName)
    }

    def foreach(callback: (DataRow) => Unit): DataSet = {
        TABLE.foreach(callback)
        this
    }

    def map(callback: (DataRow) => DataRow) : DataSet = {
        TABLE.cut(TABLE.map(callback))
        this
    }

    def table(fields: (String, DataType)*)(callback: (DataRow) => DataTable): DataSet = {
        TABLE.cut(TABLE.table(fields: _*)(callback))
        this
    }

    def flat(callback: (DataTable) => DataRow): DataSet = {
        val row = callback(TABLE)
        TABLE.clear()
        TABLE.addRow(row)
        this
    }

    def filter(callback: (DataRow) => Boolean): DataSet = {
        TABLE.cut(TABLE.filter(callback))
        this
    }

    def collect(filter: DataRow => Boolean)(map: DataRow => DataRow): DataSet = {
        TABLE.cut(TABLE.collect(filter)(map))
        this
    }

    def distinct(fieldNames: String*): DataSet = {
        TABLE.cut(TABLE.distinct(fieldNames: _*))
        this
    }

    def count(groupBy: String*): DataSet = {
        TABLE.cut(TABLE.count(groupBy: _*))
        this
    }

    def sum(fieldName: String, groupBy: String*): DataSet = {
        TABLE.cut(TABLE.sum(fieldName, groupBy: _*))
        this
    }

    def avg(fieldName: String, groupBy: String*): DataSet = {
        TABLE.cut(TABLE.avg(fieldName, groupBy: _*))
        this
    }

    def min(fieldName: String, groupBy: String*): DataSet = {
        TABLE.cut(TABLE.min(fieldName, groupBy: _*))
        this
    }

    def max(fieldName: String, groupBy: String*): DataSet = {
        TABLE.cut(TABLE.max(fieldName, groupBy: _*))
        this
    }

    def take(amount: Int): DataSet = {
        TABLE.cut(TABLE.take(amount))
        this
    }

    def insertRow(fields: (String, Any)*): DataSet = {
        TABLE.insertRow(fields: _*)
        this
    }

    def insertRowIfEmpty(fields: (String, Any)*): DataSet = {
        if (TABLE.isEmpty) {
            TABLE.insertRow(fields: _*)
        }
        this
    }

    // ---------- Json & Api ---------

    def openJson(): DataSet = {
        this
    }

    def openJson(jsonText: String): DataSet = {
        JSON = Json.fromText(jsonText)
        this
    }

    def openJsonApi(url: String): DataSet = {
        JSON = Json.fromURL(url)
        this
    }

    def openJsonApi(url: String, post: String): DataSet = {
        JSON = Json.fromURL(url, post)
        this
    }

    def find(jsonPath: String): DataSet = {
        TABLE.copy(JSON.parseTable(jsonPath))
        this
    }
    
    
    // ---------- dataSource ----------

    def executeMapList(SQL: String, values: Any*): java.util.List[java.util.Map[String, Any]] = CURRENT.executeMapList(SQL, values: _*)
    def executeDataTable(SQL: String, values: Any*): DataTable = CURRENT.executeDataTable(SQL, values: _*)
    def executeDataRow(SQL: String, values: Any*): DataRow = CURRENT.executeDataRow(SQL, values: _*)
    def executeSingleList(SQL: String, values: Any*): java.util.List[Any] = CURRENT.executeSingleList(SQL, values: _*)
    def executeSingleValue(SQL: String, values: Any*): DataCell = CURRENT.executeSingleValue(SQL, values: _*)
    def executeExists(SQL: String, values: Any*): Boolean = CURRENT.executeExists(SQL, values: _*)
    def executeNonQuery(SQL: String, values: Any*): Int = CURRENT.executeNonQuery(SQL, values: _*)
    
    // ---------- Json Basic ----------
    
    def findDataTable(jsonPath: String): DataTable = JSON.parseTable(jsonPath)
    def findDataRow(jsonPath: String): DataRow = JSON.parseRow(jsonPath)
    def findList(jsonPath: String): java.util.List[Any] = JSON.parseList(jsonPath)
    def findValue(jsonPath: String): DataCell = JSON.parseValue(jsonPath)

    // ---------- other ----------
    
    def close(): Unit = {
        SOURCES.values.foreach(_.close())
        SOURCES.clear()
        BUFFER.clear()
        TABLE.clear()
    }
}