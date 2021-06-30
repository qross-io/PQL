package io.qross.setting

import io.qross.core.{DataRow, DataType}
import io.qross.ext.TypeExt._
import io.qross.jdbc.{DBType, DataSource, JDBC}
import io.qross.net.Json
import io.qross.pql.{GlobalFunction, GlobalVariable}

object Configurations {

    private val CONFIG = new DataRow()

    load()

    def load(): Unit = {
        if (JDBC.hasQrossSystem) {
            val ds = DataSource.QROSS

            //load configurations
            if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_conf'")) {
                ds.executeDataTable("SELECT conf_key, conf_value FROM qross_conf")
                    .foreach(row => {
                        CONFIG.set(row.getString("conf_key"), row.getString("conf_value"))
                    }).clear()
            }

            //load properties
            if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_properties'")) {
                ds.executeDataTable("select * FROM qross_properties WHERE enabled='yes'")
                    .foreach(row => {
                        val path = row.getString("property_path")
                        val format = row.getString("property_format") match {
                            case "properties" => 0
                            case "yaml" | "yml" => 1
                            case "json" => 2
                            case _ => 0
                        }
                        row.getString("property_source") match {
                            case "local" => Properties.loadLocalFile(path)
                            case "resource" => Properties.loadResourcesFile(path)
                            case "nacos" => Properties.loadNacosConfig(path, format)
                            case "url" => Properties.loadUrlConfig(path, format)
                        }
                    }).clear()
            }

            if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_connections'")) {
                ds.executeDataTable("SELECT * FROM qross_connections WHERE owner=0 AND enabled='yes'")
                    .foreach(row => {
                        val databaseType = row.getString("database_type").toLowerCase().replace(" ", "")
                        if (databaseType != DBType.Redis) JDBC.connections += row.getString("connection_name") -> new JDBC(
                            databaseType,
                            row.getString("connection_string"),
                            row.getString("jdbc_driver"),
                            row.getString("username"),
                            row.getString("password"),
                            row.getInt("overtime"),
                            row.getInt("retry_limit")
                        )
                        else {
                            val connectionName = row.getString("connection_name").takeAfterIfContains("redis.")
                            Properties.set(s"redis.$connectionName.host", row.getString("host"))
                            Properties.set(s"redis.$connectionName.port", row.getString("port"))
                            Properties.set(s"redis.$connectionName.password", row.getString("password"))
                            Properties.set(s"redis.$connectionName.database", row.getString("default_database"))
                        }
                    }).clear()
            }

            if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_functions'")) {
                ds.executeDataTable("SELECT function_name, function_args, function_statement FROM qross_functions WHERE owner=0")
                    .foreach(row => {
                        val name = row.getString("function_name")
                        val args = row.getString("function_args")
                        if (args == "") GlobalFunction.WITHOUT_ARGUMENTS += name
                        GlobalFunction.SYSTEM += name -> new GlobalFunction(name, args, row.getString("function_statement"))
                        GlobalFunction.NAMES += name
                    }).clear()
            }

            if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_variables'")) {
                ds.executeDataTable("SELECT variable_group, variable_name, variable_type, variable_value FROM qross_variables WHERE owner=0")
                    .foreach(row => {
                        GlobalVariable.SYSTEM.set(row.getString("variable_name"), row.getString("variable_type") match {
                            case "TEXT" => row.getString("variable_value")
                            case "INTEGER" => row.getLong("variable_value")
                            case "DECIMAL" => row.getDouble("variable_value")
                            case "BOOLEAN" => row.getBoolean("variable_value")
                            case "DATETIME" => row.getDateTime("variable_value")
                            case "ARRAY" => Json.fromText(row.getString("variable_value")).parseJavaList("/")
                            case "ROW" => Json.fromText(row.getString("variable_value")).parseRow("/")
                            case "TABLE" => Json.fromText(row.getString("variable_value")).parseTable("/")
                            case _ => row.getString("variable_value")
                        }, new DataType(row.getString("variable_type")))

                    }).clear()
            }

            ds.close()
        }
    }

    def load(userId: Int): Unit = {
        //从数据库加载用户全局变量
        if (JDBC.hasQrossSystem && userId > 0) {
            val ds = DataSource.QROSS
            if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_connections'")) {
                ds.executeDataTable("SELECT * FROM qross_connections WHERE owner=? AND enabled='yes'", userId)
                    .foreach(row => {
                        val databaseType = row.getString("database_type").toLowerCase().replace(" ", "")
                        if (databaseType != DBType.Redis) {
                            JDBC.connections += row.getString("connection_name") -> new JDBC(
                                databaseType,
                                row.getString("connection_string"),
                                row.getString("jdbc_driver"),
                                row.getString("username"),
                                row.getString("password"),
                                row.getInt("overtime"),
                                row.getInt("retry_limit")
                            )
                        }
                        else {
                            val connectionName = row.getString("connection_name").takeAfterIfContains("redis.")
                            Properties.set(s"redis.$connectionName.host", row.getString("host"))
                            Properties.set(s"redis.$connectionName.port", row.getString("port"))
                            Properties.set(s"redis.$connectionName.password", row.getString("password"))
                            Properties.set(s"redis.$connectionName.database", row.getString("default_database"))
                        }
                    }).clear()
            }

            if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_functions'")) {
                ds.executeDataTable("SELECT function_name, function_args, function_statement FROM qross_functions WHERE owner=?", userId)
                    .foreach(row => {
                        val name = row.getString("function_name")
                        val args = row.getString("function_args")
                        if (args == "") {
                            GlobalFunction.WITHOUT_ARGUMENTS += name
                        }
                        GlobalFunction.USER += name -> new GlobalFunction(name, args, row.getString("function_statement"))
                        GlobalFunction.NAMES += name
                    }).clear()
            }

            if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_variables'")) {
                ds.executeDataTable("SELECT variable_name, variable_type, variable_value FROM qross_variables WHERE owner=?", userId)
                    .foreach(row => {
                        GlobalVariable.USER.set(
                            row.getString("variable_name"),
                            row.getString("variable_type") match {
                                case "TEXT" => row.getString("variable_value")
                                case "INTEGER" => row.getLong("variable_value")
                                case "DECIMAL" => row.getDouble("variable_value")
                                case "BOOLEAN" => row.getBoolean("variable_value")
                                case "ARRAY" => Json.fromText(row.getString("variable_value")).parseJavaList("/")
                                case "ROW" => Json.fromText(row.getString("variable_value")).parseRow("/")
                                case "TABLE" => Json.fromText(row.getString("variable_value")).parseTable("/")
                                case _ => row.getString("variable_value")
                            }, new DataType(row.getString("variable_type")))

                    }).clear()
            }

            ds.close()
        }

    }

    def contains(name: String): Boolean = {
        CONFIG.contains(name)
    }

    def get(name: String): String = {
        CONFIG.getString(name)
    }

    //只更新内存中的配置
    def set(name: String, value: Any): Unit = {
        CONFIG.set(name, value)
    }

    //更新数据库项
    def update(name: String, value: Any): Unit = {
        if (JDBC.hasQrossSystem) {
            val ds = DataSource.QROSS
            if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_conf'")) {
                ds.executeNonQuery("UPDATE qross_conf SET conf_value=? WHERE conf_key=?", value, name)
            }
            ds.close()
        }
    }

    //如果优先从配置项中获取，如果不存在, 则从Properties中获取
    def getOrProperty(name: String, propertyName: String, defaultValue: String = ""): String = {
        CONFIG.getStringOption(name).getOrElse(Properties.get(propertyName, defaultValue))
    }

    //优先从Properties中获取，如果不存在，则从配置项中获取
    def getPropertyOrElse(propertyName: String, configurationName: String, defaultValue: String = ""): String = {
        if (Properties.contains(propertyName)) {
            Properties.get(propertyName, defaultValue)
        }
        else {
            CONFIG.getStringOption(configurationName).getOrElse(defaultValue)
        }
    }

    //如果配置项不存在 ，则设置默认值
    def getOrElse(name: String, defaultValue: Any): Any = {
        CONFIG.get(name) match {
            case Some(value) => value
            case None => defaultValue
        }
    }
}