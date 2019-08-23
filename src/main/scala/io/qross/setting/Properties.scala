package io.qross.setting

import java.io._
import io.qross.jdbc.{DataSource, JDBC}
import scala.collection.JavaConverters._

object Properties {

    /*
    加载顺序
    与jar包同一目录下的 qross.properties
    与jar包同一目录下的 dbs.properties
    jar包运行参数 --properties 后的所有 properties文件 适用于worker
            jar包内的conf.properties
    数据库中的 properties / qross_properties
            数据库中的 连接串
            连接名冲突时先到先得

    将所有连接串保存在 JDBC.connections中
    properties中的连接串调取时使用
    优先使用数据库中的连接串，如果没有，再去properties找
    */

    private val props = new java.util.Properties()

    loadLocalFile(new File(BaseClass.MAIN.getProtectionDomain.getCodeSource.getLocation.getPath).getParentFile.getAbsolutePath.replace("\\", "/") + "/qross.properties")
    loadResourcesFile("/conf.properties")

    checkReferrer("mysql.qross", "jdbc.default")
    checkReferrer("jdbc.default", "mysql.qross")

    if (containsQross) {
        loadPropertiesAndConnections()
    }

    private def checkReferrer(name: String, alternate: String): Unit = {
        if (contains(name)) {
            val value = props.getProperty(name)
            if (contains(value)) {
                props.setProperty(name, props.getProperty(value))
            }
            else  if (contains(s"$value.url")) {
                props.setProperty(name, props.getProperty(s"$value.url"))
            }
        }
        else {
            props.setProperty(name, props.getProperty(alternate))
        }
    }

    def containsQross: Boolean = {
        props.containsKey("mysql.qross") || props.containsKey("mysql.qross.url")
    }

    def contains(key: String): Boolean = {
        props.containsKey(key)
    }

    def get(key: String, defaultValue: String): String = {
        if (props.containsKey(key)) {
            props.getProperty(key)
        }
        else {
            defaultValue
        }
    }

    def get(key: String): String = {
        get(key, "null")
    }

    def set(key: String, value: String): Unit = {
        props.setProperty(key, value)
    }

    def loadLocalFile(path: String): Unit = {
        val file = new File(path)
        if (file.exists()) {
            props.load(new BufferedInputStream(new FileInputStream(file)))
        }
    }

    def loadResourcesFile(path: String): Unit = {
        try {
            props.load(new BufferedReader(new InputStreamReader(BaseClass.MAIN.getResourceAsStream(path))))
        }
        catch {
            case _ : Exception =>
        }
    }

    //加载保存在数据库中的properties文件
    def loadPropertiesAndConnections(): Unit = {
        val ds = DataSource.QROSS

        ds.executeDataTable("SELECT properties_type, properties_path FROM qross_properties WHERE enabled='yes' ORDER BY id ASC")
                        .foreach(row => {
                            if (row.getString("properties_type") == "local") {
                                loadLocalFile(row.getString("properties_path"))
                            }
                            else {
                                loadResourcesFile(row.getString("properties_path"))
                            }
                        }).clear()

        ds.executeDataTable("SELECT * FROM qross_connections WHERE enabled='yes'")
                        .foreach(row => {
                            //从数据行新建
                            JDBC.connections += row.getString("connection_name") -> new JDBC(
                                row.getString("db_type"),
                                row.getString("connection_string"),
                                row.getString("jdbc_driver"),
                                row.getString("username"),
                                row.getString("password"),
                                row.getInt("overtime"),
                                row.getInt("retry_limit")
                            )
                        }).clear()

        ds.close()
    }
}
