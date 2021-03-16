package io.qross.setting

import io.qross.core.DataRow
import io.qross.ext.TypeExt._
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.pql.{GlobalFunction, GlobalVariable}

object Configurations {

    private val CONFIG = new DataRow()

    load()

    def load(): Unit = {
        if (JDBC.hasQrossSystem) {
            val ds = DataSource.QROSS

            //load configurations
            ds.executeDataTable("SELECT conf_key, conf_value FROM qross_conf")
                .foreach(row => {
                    CONFIG.set(row.getString("conf_key"), row.getString("conf_value"))
                }).clear()

            //load properties
            ds.executeDataTable("select * FROM qross_properties WHERE enabled='yes'")
                .foreach(row => {
                    val path = row.getString("property_path")
                    val format = {
                        row.getString("property_format") match {
                            case "properties" => 0
                            case "yaml" | "yml" => 1
                            case "json" => 2
                            case _ => 0
                        }
                    }
                    row.getString("property_source") match {
                        case "local" => Properties.loadLocalFile(path)
                        case "resource" => Properties.loadResourcesFile(path)
                        case "nacos" => Properties.loadNacosConfig(path, format)
                        case "url" => Properties.loadUrlConfig(path, format)
                    }
                }).clear()

            ds.executeDataTable("SELECT * FROM qross_connections WHERE connection_owner=0 AND enabled='yes'")
                .foreach(row => {
                    val databaseName = row.getString("database_name")
                    if (databaseName != "redis") {
                        JDBC.connections += row.getString("connection_name") -> new JDBC(
                            databaseName,
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

            ds.executeDataTable("SELECT function_name, function_args, function_statement FROM qross_functions WHERE function_owner=0")
                .foreach(row => {
                    val name = row.getString("function_name")
                    val args = row.getString("function_args")
                    if (args == "") {
                        GlobalFunction.WITHOUT_ARGUMENTS += name
                    }
                    GlobalFunction.SYSTEM += name -> new GlobalFunction(name, args, row.getString("function_statement"))
                    GlobalFunction.NAMES += name
                }).clear()

            ds.executeDataTable("SELECT var_group, var_name, var_type, var_value FROM qross_variables WHERE var_owner=0")
                .foreach(row => {
                    GlobalVariable.SYSTEM.set(
                        row.getString("var_name").toUpperCase()
                        , row.getString("var_type") match {
                            case "INTEGER" => row.getLong("var_value")
                            case "DECIMAL" => row.getDouble("var_value")
                            case _ => row.getString("var_value")
                        })

                }).clear()

            ds.close()
        }
    }

    def load(userId: Int): Unit = {
        //从数据库加载用户全局变量
        if (JDBC.hasQrossSystem && userId > 0) {
            val ds = DataSource.QROSS

            ds.executeDataTable("SELECT * FROM qross_connections WHERE connection_owner=? AND enabled='yes'", userId)
                .foreach(row => {
                    val databaseName = row.getString("database_name")
                    if (databaseName != "redis") {
                        JDBC.connections += row.getString("connection_name") -> new JDBC(
                            databaseName,
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

            ds.executeDataTable("SELECT function_name, function_args, function_statement FROM qross_functions WHERE function_owner=?", userId)
                .foreach(row => {
                    val name = row.getString("function_name")
                    val args = row.getString("function_args")
                    if (args == "") {
                        GlobalFunction.WITHOUT_ARGUMENTS += name
                    }
                    GlobalFunction.USER += name -> new GlobalFunction(name, args, row.getString("function_statement"))
                    GlobalFunction.NAMES += name
                }).clear()

            ds.executeDataTable("SELECT var_name, var_type, var_value FROM qross_variables WHERE var_owner=?", userId)
                .foreach(row => {
                    GlobalVariable.USER.set(
                        row.getString("var_name").toUpperCase(),
                        row.getString("var_type") match {
                            case "INTEGER" => row.getLong("var_value")
                            case "DECIMAL" => row.getDouble("var_value")
                            case _ => row.getString("var_value")
                        })

                }).clear()

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
            DataSource.QROSS.queryUpdate("UPDATE qross_conf SET conf_value=? WHERE conf_key=?", value, name)
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