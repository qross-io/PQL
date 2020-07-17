package io.qross.fql

import io.qross.core.{DataHub, DataTable, DataType}
import io.qross.fs.{FileReader, FileWriter, TextFile}
import io.qross.pql.{PQL, Syntax}
import io.qross.ext.TypeExt._

import scala.collection.mutable

// FQL = File/Fragment Query Language
// For manipulating Excel/TXT/HDFS ......

class FQL(dh: DataHub, PQL: PQL) {

    def this() {
        this(null, null)
    }

    def this(dh: DataHub) {
        this(dh, null)
    }

    def this(PQL: PQL) {
        this(PQL.dh, PQL)
    }

    //无数据信息存在各自的表中
    //private[qross] val METADATA = new mutable.HashMap[String, mutable.LinkedHashMap[String, DataType]]()
    private[qross] val TABLES = new mutable.HashMap[String, Any]()  //虚表
    private[qross] val ALIASES = new mutable.HashMap[String, String]() //表别名
    private[qross] var recentTableName = ""

    private[qross] def create(tableName: String, source: Any): Unit = {
        if (!ALIASES.contains(tableName.toLowerCase())) {
            TABLES += tableName -> source
        }
        else {
            TABLES += ALIASES(tableName.toLowerCase()) -> source
        }
        recentTableName = tableName
    }

    def getTable: Any = {
        if (TABLES.contains(recentTableName)) {
            TABLES(recentTableName)
        }
        else {
            TABLES(ALIASES(recentTableName.toLowerCase()))
        }
    }

    def Table[T](tableName: String): T = {
        if (TABLES.contains(tableName)) {
            TABLES(tableName).asInstanceOf[T]
        }
        else {
            TABLES(ALIASES(tableName.toLowerCase())).asInstanceOf[T]
        }
    }

    def select(SQL: String, values: Any*): DataTable = {

        //替换参数 values
        //解析语句并生成执行计划，类似条件解析的分步

        val SELECT = new SELECT(SQL)

        val from = SELECT.from
        val symbol = from.take(1)

        if (symbol == ":") {
            TABLES(ALIASES(from.drop(1).toLowerCase())).asInstanceOf[TextFile].select(SELECT)
        }
        else {
            new DataTable()
        }
    }

    def insert(SQL: String, values: Any*): Int = {
        0
    }

    def tableUpdate(SQL: String, table: DataTable): Int = {
        0
    }

    def tableSelect(SQL: String, table: DataTable): DataTable = {
        null
    }

    def close(): Unit = {
        TABLES.values.foreach {
            case file: TextFile => file.close()
            case reader: FileReader => reader.close()
            case writer: FileWriter => writer.close()
            case _ =>
        }
    }
}
