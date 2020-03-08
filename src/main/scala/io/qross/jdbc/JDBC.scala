package io.qross.jdbc

import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.setting.Properties

import scala.collection.{immutable, mutable}
import scala.util.control.Breaks.breakable

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
                DBType.SQLite -> "org.sqlite.JDBC",
                DBType.Presto -> "com.facebook.presto.jdbc.PrestoDriver",
                DBType.Hive -> "org.apache.hive.jdbc.HiveDriver",
                DBType.Oracle -> "oracle.jdbc.driver.OracleDriver",
                DBType.Impala -> "org.apache.hive.jdbc.HiveDriver,com.cloudera.impala.jdbc4.Driver",
                DBType.SQLServer -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                DBType.Memory -> "org.sqlite.JDBC")
                DBType.AnalyticDB -> ""

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
            else {
                //Output.writeWarning(s"Can't find properties key $QROSS, it must be set in .properties files.")
            }
        }

        if (QrossExists && !QrossConnectable) {

            if (!DataSource.QROSS.queryTest()) {
                Output.writeException(s"Can't open database, please check your connection string of $QROSS.")
            }
            else {
                var version: String = ""
                try {
                    version = DataSource.QROSS.querySingleValue("SELECT conf_value FROM qross_conf WHERE conf_key='QROSS_VERSION'").asText
                    QrossConnectable = true
                }
                catch {
                    case e: Exception => e.printStackTrace()
                }

                if (version == "") {
                    Output.writeException("Can't find Qross system, please create your Qross system use Qross Master.")
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
                    if (connectionName.contains(name) || connectionString.contains(name)) {
                        dbType = name
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

class JDBC(var dbType: String,
           var connectionString: String = "",
           var driver: String = "",
           var username: String = "",
           var password: String = "",
           var overtime: Int = 0, //超时时间, 0 不限制
           var retryLimit: Int = 0 //重试次数, 0 不重试
          ) {

    var alternativeDriver: String = ""

    if (driver.contains(",")) {
        alternativeDriver = driver.takeAfter(",")
        driver = driver.takeBefore(",")
    }

    if (dbType == DBType.SQLite && !connectionString.startsWith("jdbc:sqlite:")) {
        connectionString = "jdbc:sqlite:" + connectionString
    }

    if (overtime == 0) {
        dbType match {
            case DBType.MySQL | DBType.SQLServer | DBType.PostgreSQL | DBType.Oracle =>
                overtime = 10000
            case _ =>
        }
    }
    if (retryLimit == 0) {
        dbType match {
            case DBType.MySQL | DBType.SQLServer | DBType.PostgreSQL | DBType.Oracle =>
                retryLimit = 100
            case DBType.Hive | DBType.Spark | DBType.Impala | DBType.Presto =>
                retryLimit = 3
            case _ =>
        }
    }
}
