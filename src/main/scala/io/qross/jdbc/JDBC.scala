package io.qross.jdbc

import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.setting.Properties

import scala.collection.{immutable, mutable}
import scala.util.control.Breaks._

object DBType {
    val MySQL = "mysql"
    val SQLServer = "sqlserver"
    val Hive = "hive"
    val Memory = "sqlite.memory"
    val SQLite = "sqlite"
    val Oracle = "oracle"
    val PostgreSQL = "postgresql"
    val Impala = "impala"
    val Spark = "spark"
    val Presto = "presto"
    val AnalyticDB = "analyticdb"
}

object JDBC {

    val QROSS = "mysql.qross"
    val DEFAULT = "jdbc.default"

    private var QrossExists = false
    private var QrossConnectable = false

    private val drivers: immutable.HashMap[String, String] = immutable.HashMap[String, String](
                DBType.MySQL -> "com.mysql.cj.jdbc.Driver,com.mysql.jdbc.Driver", //com.mysql.cj.jdbc.Driver,com.mysql.jdbc.Driver",
                DBType.PostgreSQL -> "org.postgresql.Driver",
                DBType.SQLite -> "org.sqlite.JDBC",
                DBType.Presto -> "com.facebook.presto.jdbc.PrestoDriver",
                DBType.Hive -> "org.apache.hive.jdbc.HiveDriver",
                DBType.Oracle -> "oracle.jdbc.driver.OracleDriver",
                DBType.Impala -> "org.apache.hive.jdbc.HiveDriver,com.cloudera.impala.jdbc4.Driver",
                DBType.SQLServer -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                DBType.Memory -> "org.sqlite.JDBC")
                DBType.AnalyticDB -> "com.mysql.cj.jdbc.Driver,com.mysql.jdbc.Driver"

    //已保存的数据库连接信息
    val connections = new mutable.HashMap[String, JDBC]()
    connections += DBType.Memory -> new JDBC(DBType.Memory, "jdbc:sqlite::memory:", "org.sqlite.JDBC")

    def Qross: JDBC = {
        if (!connections.contains(QROSS)) {
            connections += QROSS -> new JDBC(DBType.MySQL, Properties.get(QROSS), drivers(DBType.MySQL))
        }
        connections(QROSS)
    }

    def hasQrossSystem: Boolean = {

        if (!QrossExists) {
            if (Properties.contains(QROSS) || Properties.contains(QROSS + ".url")) {
                QrossExists = true
            }
        }

        if (QrossExists && !QrossConnectable) {
            if (!DataSource.QROSS.queryTest()) {
                Output.writeException(s"Can't open database, please check your connection string of $QROSS.")
            }
            else {
                if (DataSource.QROSS.queryExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_conf'")) {
                    QrossConnectable = true
                }
            }
        }

        QrossExists && QrossConnectable
    }

    //从Properties新建
    def take(connectionName: String): JDBC = {
        var dbType = ""
        var connectionString = ""
        var driver = ""
        var username = ""
        var password = ""
        var overtime = 0
        var retry = 0


        if (Properties.contains(connectionName)) {
            connectionString = Properties.get(connectionName)
        }
        else if (Properties.contains(connectionName + ".url")) {
            connectionString = Properties.get(connectionName + ".url")
        }
        else if (connectionName.endsWith(".sqlite")) {
            dbType = DBType.SQLite
            connectionString = "jdbc:sqlite:" + connectionName
            driver = "org.sqlite.JDBC"
        }
        else if (connectionName.startsWith("jdbc:")) {
            connectionString = connectionName
        }
        else {
            throw new Exception(s"""Can't find connection string of connection name "$connectionName" in properties.""")
        }

        if (dbType == "") {
            breakable {
                for (name <- JDBC.drivers.keySet) {
                    if (connectionName.contains(name) || connectionString.contains(name) || driver.contains(name)) {
                        dbType = name
                        break
                    }
                }
            }
        }

        if (driver == "") {
            if (Properties.contains(connectionName + ".driver")) {
                driver = Properties.get(connectionName + ".driver")
            }

            if (driver == "") {
                breakable {
                    for (name <- JDBC.drivers.keySet) {
                        if (connectionName.contains(name) || connectionString.contains(name)) {
                            driver = JDBC.drivers(name)
                            break
                        }
                    }
                }
            }
        }

        if (driver == "") {
            throw new Exception("Can't match any driver to open database. Please specify the driver of connection string use property name \"connectionName.driver\"")
        }

        if (Properties.contains(connectionName + ".username")) {
            username = Properties.get(connectionName + ".username")
        }

        if (Properties.contains(connectionName + ".password")) {
            password = Properties.get(connectionName + ".password")
        }

        if (Properties.contains(connectionName + ".overtime")) {
            overtime = Properties.get(connectionName + ".overtime").toInt
        }

        if (Properties.contains(connectionName + ".retry")) {
            retry = Properties.get(connectionName + ".retry").toInt
        }

        new JDBC(dbType, connectionString, driver, username, password, overtime, retry)
    }

    def add(connectionName: String, connectionString: String): Unit = {
        add(connectionName, "", connectionString, "", "")
    }

    def add(connectionName: String, driver: String, connectionString: String): Unit = {
        add(connectionName, connectionString, driver, "", "")
    }

    def add(connectionName: String, driver: String, connectionString: String, username: String, password: String): Unit = {
        var dbType = ""
        var alterDriver = driver

        breakable {
            for (name <- JDBC.drivers.keySet) {
                if (connectionName.contains(name) || connectionString.contains(name) || driver.contains(name)) {
                    dbType = name
                }
            }
        }

        if (alterDriver == "") {
            breakable {
                for (name <- JDBC.drivers.keySet) {
                    if (connectionName.contains(name) || connectionString.contains(name)) {
                        alterDriver = JDBC.drivers(name)
                        break
                    }
                }
            }
        }

        connections += connectionName -> new JDBC(dbType, connectionString, alterDriver, username, password, 0, 3)
    }

    def get(connectionName: String): JDBC = {
        if (!connections.contains(connectionName)) {
            connections += connectionName -> take(connectionName)
        }

        connections(connectionName)
    }

    //refresh
    def setup(id: Int): Unit = {
        if (hasQrossSystem) {
            val connection = DataSource.QROSS.queryDataRow("SELECT * FROM qross_connections WHERE id=?", id)
            connections.put(connection.getString("connection_name"), new JDBC(
                connection.getString("db_type"),
                connection.getString("connection_string"),
                connection.getString("jdbc_driver"),
                connection.getString("username"),
                connection.getString("password"),
                connection.getInt("overtime"),
                connection.getInt("retry_limit")
            ))
        }
    }

    def remove(connectionName: String): Unit = {
        connections.remove(connectionName)
    }
}

class JDBC(val dbType: String,
           $connectionString: String = "",
           $driver: String = "",
           val username: String = "",
           val password: String = "",
           $overtime: Int = 0, //超时时间, 0 不限制
           $retryLimit: Int = 0 //重试次数, 0 不重试
          ) {

    val (driver: String, alternativeDriver: String) = {
        if ($driver.contains(",")) {
            ($driver.takeBefore(","), $driver.takeAfter(","))
        }
        else {
            ($driver, "")
        }
    }

    val connectionString: String = {
        if (dbType == DBType.SQLite && !$connectionString.startsWith("jdbc:sqlite:")) {
            "jdbc:sqlite:" + $connectionString
        }
        else {
            $connectionString
        }
    }

    val overtime: Int = {
        if ($overtime == 0) {
            dbType match {
                case DBType.MySQL | DBType.SQLServer | DBType.PostgreSQL | DBType.Oracle => 10000
                case _ => 0
            }
        }
        else {
            $overtime
        }
    }

    val retryLimit: Int = {
        if ($retryLimit == 0) {
            dbType match {
                case DBType.MySQL | DBType.SQLServer | DBType.PostgreSQL | DBType.Oracle => 100
                case DBType.Hive | DBType.Spark | DBType.Impala | DBType.Presto => 3
                case _ => 0
            }
        }
        else {
            $retryLimit
        }
    }
}
