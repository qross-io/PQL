package io.qross.fql

import io.qross.core.{DataHub, DataTable}
import io.qross.fs.TextFile
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

    private[qross] val TABLES = new mutable.HashMap[String, Any]()  //虚表
    private[qross] val ALIASES = new mutable.HashMap[String, String]()
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

        //暂时用Syntax解析, 先实现基本功能
        val plan = Syntax("SELECT").plan(SQL.takeAfter("\\s".r).trim())

        val from = plan.oneArgs("FROM")
        val symbol = from.take(1)

        if (symbol == ":") {
            val file = TABLES(ALIASES(from.drop(1))).asInstanceOf[TextFile]
            if (plan.contains("SEEK")) {
                file.seek(plan.oneArgs("SEEK").toInteger(0))
            }

            if (plan.contains("LIMIT")) {
                val limit = plan.limitArgs("LIMIT")
                val (m, n) = {
                    if (limit._2 == "") {
                        (limit._1.toInteger(10).toInt, -1)
                    }
                    else {
                        (limit._1.toInteger(0).toInt, limit._2.toInteger(-1).toInt)
                    }
                }
                if (n > -1) {
                    file.limit(m, n)
                }
                else {
                    file.limit(m)
                }
            }

            file.select(plan.selectArgs(""): _*)
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
            case _ =>
        }
    }
}
