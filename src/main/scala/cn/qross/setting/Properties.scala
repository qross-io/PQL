package cn.qross.setting

import java.io._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import cn.qross.ext.TypeExt._
import cn.qross.fs.{FileReader, ResourceFile}
import cn.qross.jdbc.{DataSource, JDBC}
import cn.qross.net.{Http, Json}
import org.yaml.snakeyaml.Yaml

import scala.xml._

object Properties {

    /*
    加载顺序
    与jar包同一目录下的 qross.properties
    jar包运行参数 --properties 后的所有 properties文件 适用于worker
            jar包内的conf.properties
    数据库中的 properties
            数据库中的 连接串
            连接名冲突时后到先得

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
    //load from other
    loadCenterConfig()

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

    def getAllProperties: java.util.Properties = {
        props
    }

    def get(key: String): String = {
        props.getProperty(key)
    }

    def set(key: String, value: String): Unit = {
        props.setProperty(key, value)
    }

    def remove(key: String): Unit = {
        props.remove(key)
    }

    def keys: Array[java.lang.Object] = props.keySet().toArray

    def load(id: Int): Unit = {
        if (JDBC.hasQrossSystem) {
            val ds = DataSource.QROSS
            if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_properties'")) {
                val property = ds.executeDataRow("SELECT * FROM qross_properties WHERE id=?", id)
                val path = property.getString("property_path")
                val format = {
                    property.getString("property_format") match {
                        case "properties" => 0
                        case "yaml" | "yml" => 1
                        case "json" => 2
                        case _ => 0
                    }
                }
                property.getString("property_source") match {
                    case "local" => Properties.loadLocalFile(path)
                    case "resource" => Properties.loadResourcesFile(path)
                    case "nacos" => Properties.loadNacosConfig(path, format)
                    case "url" => Properties.loadUrlConfig(path, format)
                }
            }
            ds.close()
        }
    }

    def loadConfig(content: String, format: Int = Config.Properties): Unit = {
        format match {
            case Config.Properties => props.load(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content.getBytes()))))
            case Config.Yaml => recurseYamlMap(new Yaml().load[java.util.LinkedHashMap[String, Any]](content))
            case Config.Json => recurseJsonNode(Json.fromText(content).findNode("/"))
        }
    }

    def loadLocalFile(path: String, loadResources: Boolean = true): Unit = {
        val file = new File(path)
        if (file.exists()) {
            path.takeAfterLast(".").toLowerCase() match {
                case "properties" => props.load(new BufferedInputStream(new FileInputStream(file)))
                case "yaml" | "yml" => recurseYamlMap(new Yaml().load[java.util.LinkedHashMap[String, Any]](new FileInputStream(file)))
                case _ => loadConfig(new FileReader(path).readToEnd, Config.Json)
            }
        }
        else if (loadResources) {
            loadResourcesFile(path)
        }
    }

    def loadLocalFile(path: String, format: Int): Unit = {
        val file = new File(path)
        if (file.exists()) {
            format match {
                case Config.Properties => props.load(new BufferedInputStream(new FileInputStream(file)))
                case Config.Yaml => recurseYamlMap(new Yaml().load[java.util.LinkedHashMap[String, Any]](new FileInputStream(file)))
                case _ => loadConfig(new FileReader(path).readToEnd, Config.Json)
            }
        }
        else {
            loadResourcesFile(path, format)
        }
    }

    def loadResourcesFile(path: String): Unit = {
        try {
            path.takeAfterLast(".").toLowerCase() match {
                case "properties" => props.load(new BufferedReader(new InputStreamReader(BaseClass.MAIN.getResourceAsStream(path))))
                case "yaml" | "yml" => recurseYamlMap(new Yaml().load[java.util.LinkedHashMap[String, Any]](BaseClass.MAIN.getResourceAsStream(path)))
                case _ => loadConfig(ResourceFile.open(path).content, Config.Json)
            }
        }
        catch {
            case e: Exception =>
                //println(s"""File $path doesn't exists. Please check "/out/production/resources/" or "/build/resources/" if you are developing with Intellij Idea.""")
                //e.printStackTrace()
        }
    }

    def loadResourcesFile(path: String, format: Int): Unit = {
        try {
            format match {
                case Config.Properties => props.load(new BufferedReader(new InputStreamReader(BaseClass.MAIN.getResourceAsStream(path))))
                case Config.Yaml => recurseYamlMap(new Yaml().load[java.util.LinkedHashMap[String, Any]](BaseClass.MAIN.getResourceAsStream(path)))
                case _ => loadConfig(ResourceFile.open(path).content, Config.Json)
            }
        }
        catch {
            case e: Exception => e.printStackTrace()
        }
    }

    def loadNacosConfig(path: String, format: Int = Config.Properties): Unit = {
        // host:port:group:data_id
        try {
            val config = path.trim().split(":")
            if (config.length == 4) {
                loadUrlConfig(s"http://${config(0).trim()}:${config(1).trim()}/nacos/v1/cs/configs?dataId=${config(3).trim()}&group=${config(2).trim()}", format)
            }
        }
        catch {
            case e: Exception => e.printStackTrace()
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

    def loadSiblingFile(fileName: String): Unit = loadLocalFile(Environment.runningDirectory + fileName, loadResources = false)

    def loadUrlConfig(url: String, format: Int = Config.Properties): Unit = loadConfig(Http.GET(url).request(), format)

    //Ensure class Properties is always loaded first

    private def recurseYamlMap(map: java.util.LinkedHashMap[String, Any], prefix: String = ""): Unit = {
        map.forEach((key, value) => {
            value match {
                case str: String => props.setProperty(prefix + key, str)
                case o =>
                    val value = o.toString
                    if (value.bracketsWith("{", "}")) {
                        recurseYamlMap(o.asInstanceOf[java.util.LinkedHashMap[String, Any]], prefix + key + ".")
                    }
                    else {
                        props.setProperty(prefix + key, value)
                    }
            }
        })
    }

    private def recurseJsonNode(node: JsonNode, prefix: String = ""): Unit = {
        if (node.isObject) {
            node.fields().forEachRemaining(item => {
                if (item.getValue.isObject) {
                    recurseJsonNode(item.getValue, prefix + item.getKey + ".")
                }
                else if (item.getValue.isTextual) {
                    props.setProperty(prefix + item.getKey, item.getValue.textValue())
                }
                else {
                    props.setProperty(prefix + item.getKey, item.getValue.toString())
                }
            })
        }
    }

    private def loadCenterConfig(): Unit = {
        if (Properties.contains("file.properties")) {
            Properties.get("file.properties")
                .split(';')
                .map(_.trim())
                .filter(_ != "")
                .foreach(file => {
                    loadLocalFile(file)
                })
        }

        if (Properties.contains("file.yaml")) {
            Properties.get("file.yaml")
                .split(';')
                .map(_.trim())
                .filter(_ != "")
                .foreach(file => {
                    loadLocalFile(file)
                })
        }

        if (Properties.contains("file.json")) {
            Properties.get("file.json")
                .split(';')
                .map(_.trim())
                .filter(_ != "")
                .foreach(file => {
                    loadLocalFile(file)
                })
        }

        if (Properties.contains("nacos.properties")) {
            Properties.get("nacos.properties")
                .split(';')
                .map(_.trim())
                .filter(_ != "")
                .foreach(nacos => {
                    loadNacosConfig(nacos, Config.Properties)
                })
        }

        if (Properties.contains("nacos.yaml")) {
            Properties.get("nacos.yaml")
                .split(';')
                .map(_.trim())
                .filter(_ != "")
                .foreach(nacos => {
                    loadNacosConfig(nacos, Config.Yaml)
                })
        }

        if (Properties.contains("nacos.json")) {
            Properties.get("nacos.json")
                .split(';')
                .map(_.trim())
                .filter(_ != "")
                .foreach(nacos => {
                    loadNacosConfig(nacos, Config.Json)
                })
        }

        if (Properties.contains("url.properties")) {
            Properties.get("url.properties")
                .split(';')
                .map(_.trim())
                .filter(_ != "")
                .foreach(url => {
                    loadUrlConfig(url, Config.Properties)
                })
        }

        if (Properties.contains("url.yaml")) {
            Properties.get("url.yaml")
                .split(';')
                .map(_.trim())
                .filter(_ != "")
                .foreach(url => {
                    loadUrlConfig(url, Config.Yaml)
                })
        }

        if (Properties.contains("url.json")) {
            Properties.get("url.json")
                .split(';')
                .map(_.trim())
                .filter(_ != "")
                .foreach(url => {
                    loadUrlConfig(url, Config.Json)
                })
        }
    }

    override def toString: String = {
        new ObjectMapper().writeValueAsString(props)
    }
    def clear(): Unit = {
        props.clear()
    }
}