package io.qross.setting

import java.io._

import io.qross.jdbc.{DataSource, JDBC}
import io.qross.ext.TypeExt._
import io.qross.fs.ResourceFile

import scala.xml._

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

    //load qross config
    loadSiblingFile("qross.properties")
    //load default config
    loadResourcesFile("/conf.properties")
    //load mybatis config
    loadMyBatisConfigEnvironments()

    //default connection
    if (!contains("jdbc.default") && containsQross) {
        props.setProperty("jdbc.default", props.getProperty("mysql.qross"))
    }

    def containsQross: Boolean = {
        props.containsKey("mysql.qross") || props.containsKey("mysql.qross.url")
    }

    def contains(key: String): Boolean = {
        props.containsKey(key)
    }

    def get(key: String, defaultValue: String): String = {
        props.getProperty(key, defaultValue)
    }

    def getAllPropertyNames: java.util.Set[String] = {
        props.stringPropertyNames()
    }

    def get(key: String): String = {
        props.getProperty(key)
    }

    def set(key: String, value: String): Unit = {
        props.setProperty(key, value)
    }

    def keys: Array[java.lang.Object] = props.keySet().toArray

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
            case e : Exception => e.printStackTrace()
        }
    }

    def loadMyBatisConfigEnvironments(path: String = "/mybatis-config.xml"): Unit = {

        val xml = BaseClass.MAIN.getResource(path)
        if (xml != null) {
            val config = XML.load(xml)
            val default = config \ "environments" \ "@default"

            (config \ "environments" \\ "environment").foreach(environment => {
                val id = environment \ "@id"
                if (id.nonEmpty) {
                    (environment \ "dataSource" \\ "property").foreach(property => {
                        val name = property \ "@name"
                        val value = property \ "@value"
                        if (name.nonEmpty && value.nonEmpty && !value.toString().bracketsWith("${", "}")) {
                            Properties.set(s"$id.$name", value.toString())

                            if (default.nonEmpty && default.toString() == id.toString()) {
                                Properties.set(s"jdbc.default.$name", value.toString())
                            }
                        }
                    })
                }
            })
        }
    }

    def loadSiblingFile(fileName: String): Unit = loadLocalFile(Environment.runningDirectory + fileName)
}