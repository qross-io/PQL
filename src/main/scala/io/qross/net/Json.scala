package io.qross.net

import java.io.InputStream
import java.net.{HttpURLConnection, MalformedURLException, URL}
import java.util

import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import io.qross.core._
import io.qross.ext.TypeExt._

import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}


object Json {
    def fromText(text: String): Json = Json(text)
    def fromURL(url: String, post: String = ""): Json = Json().readURL(url, post)

    def serialize(obj: AnyRef): String = {
        implicit val formats: Formats = Serialization.formats(NoTypeHints)
        Serialization.write(obj)
    }

    //对象列表
    val OBJECT$ARRAY: Regex = """^\[\s*\{.*\}\s*\]$""".r

    implicit class DataHub$Json(val dh: DataHub) {
        // ---------- Json & Api ---------

        def JSON: Json = {
            dh.pick[Json]("JSON") match {
                case Some(json) => json
                case None => throw new ExtensionNotFoundException("Must open a json string or api first.")
            }
        }

        def openJson(): DataHub = {
            dh
        }

        def openJson(jsonText: String): DataHub = {
            dh.plug("JSON", Json.fromText(jsonText))
        }

        def openJsonApi(url: String): DataHub = {
            dh.plug("JSON", Json.fromURL(url))
        }

        def openJsonApi(url: String, post: String): DataHub = {
            dh.plug("JSON", Json.fromURL(url, post))
        }

        def parse(jsonPath: String): DataHub = {
            dh.clear().merge(JSON.parseTable(jsonPath))
        }

        // ---------- Json Basic ----------

        def parseTable(jsonPath: String): DataTable = JSON.parseTable(jsonPath)
        def parseRow(jsonPath: String): DataRow = JSON.parseRow(jsonPath)
        def parseList(jsonPath: String): java.util.List[Any] = JSON.parseJavaList(jsonPath)
        def parseValue(jsonPath: String): DataCell = JSON.parseValue(jsonPath)
        def parseNode(jsonPath: String): JsonNode = JSON.findNode(jsonPath)
    }
}

case class Json(text: String = "") {
    
    private val mapper = new ObjectMapper
    private var root: JsonNode = _
    
    if (text != "") {
        root = mapper.readTree({
            if (text.bracketsWith("'{", "}'")
                || text.bracketsWith("\"{", "}\"")
                || text.bracketsWith("'[", "]'")
                || text.bracketsWith("\"[", "]\"")) {
                text.removeQuotes()
            }
            else {
                text
            }
        })
    }
    
    def readURL(url: String, post: String = ""): Json = {
        try {
            val URL = new URL(if (url.contains("://")) url else "http://" + url)
            if (post == "") {
                root = mapper.readTree(URL)
            }
            else {
                //Http.POST(url, post).toJson
                val conn = URL.openConnection().asInstanceOf[HttpURLConnection]
                conn.setDoOutput(true)
                conn.setDoInput(true)
                conn.addRequestProperty("Content-Type", "application/json; charset=utf-8");
                conn.setRequestMethod("POST")
                conn.connect()
    
                val os = conn.getOutputStream
                os.write(post.getBytes("utf-8"))
                os.close()
    
                val is = conn.getInputStream
                root = mapper.readTree(is)
                is.close()
            }
        }
        catch {
            case e: MalformedURLException => e.printStackTrace()
            case o: Exception => o.printStackTrace()
        }
        this
    }
    
    def readStream(inputStream: InputStream): Json = {
        root = mapper.readTree(inputStream)
        this
    }

    def parseTable(path: String): DataTable = {
        val table = new DataTable
        
        val node = findNode(path)
        if (node.isArray) {
            node.elements().forEachRemaining(child => {
                val row = new DataRow()
                if (child.isObject) {
                    child.fields().forEachRemaining(item => {
                        //table.addField(item.getKey, DataType.from(node))
                        row.set(item.getKey, getCell(item.getValue))
                    })
                }
                else if (child.isArray)  {
                    child.elements().forEachRemaining(item => {
                        //table.addField("c" + row.size, DataType.from(item))
                        row.set("c" + row.size, getCell(item))
                    })
                }
                else {
                    //table.addField("value", DataType.from(child))
                    row.set("_array", getCell(child))
                }
                table.addRow(row)
            })
            if (table.isEmpty) {
                table.addField("empty", DataType.NULL)
            }
        }
        else if (node.isObject) {
            val row = new DataRow()
            node.fields().forEachRemaining(child => {
                //table.addField(child.getKey, DataType.from(child.getValue))
                row.set(child.getKey, getCell(child.getValue))
            })
            table.addRow(row)
        }
        else {
            val row = new DataRow()
            row.set("_value", getCell(node))
            table.addRow(row)
        }
        
        table
    }

    def parseRow(path: String): DataRow = {
        
        val row = new DataRow()
        
        val node = findNode(path)
        if (node.isObject) {
            node.fields().forEachRemaining(child => {
                row.set(child.getKey, getCell(child.getValue))
            })
        }
        else if (node.isArray) {
            node.elements().forEachRemaining(child => {
                row.set("c" + row.size, getCell(child))
            })
        }
        else {
            row.set("value", getCell(node))
        }
        
        row
    }

    def parseMap(path: String): Map[String, String] = {
        val map = new mutable.LinkedHashMap[String, String]()

        val node = findNode(path)
        if (node.isObject) {
            node.fields().forEachRemaining(child => {
                map.put(child.getKey, child.getValue.asText())
            })
        }
        else {
            throw new JsonParseException("Wrong input format. The json path must point to a Object.")
        }

        map.toMap
    }

    def parseJavaMap(path: String): java.util.Map[String, String] = {
        val map = new util.HashMap[String, String]()

        val node = findNode(path)
        if (node.isObject) {
            node.fields().forEachRemaining(child => {
                if (child.getValue.isTextual) {
                    map.put(child.getKey, child.getValue.asText())
                }
                else {
                    map.put(child.getKey, child.getValue.toString)
                }
            })
        }
        else {
            throw new JsonParseException("Wrong input format. The json path must point to a Object.")
        }

        map
    }
    
    def parseJavaList(path: String): java.util.List[Any] = {
        val list = new java.util.ArrayList[Any]()
        
        val node = findNode(path)
        if (node.isArray) {
            node.elements().forEachRemaining(child => {
                list.add(getCell(child).value)
            })
        }
        else {
            list.add(getCell(node).value)
        }
        
        list
    }

    def parseDataCellList(path: String): List[DataCell] = {
        val list = new mutable.ListBuffer[DataCell]()

        val node = findNode(path)
        if (node.isArray) {
            node.elements().forEachRemaining(child => {
                list += getCell(child)
            })
        }
        else {
            list += getCell(node)
        }

        list.toList
    }

    def parseValue(path: String): DataCell = {
        getCell(findNode(path))
    }

    private def getCell(node: JsonNode): DataCell = {
        node.getNodeType match {
            case JsonNodeType.STRING | JsonNodeType.BINARY => DataCell(node.textValue(), DataType.TEXT)
            case JsonNodeType.ARRAY | JsonNodeType.OBJECT | JsonNodeType.POJO => DataCell(node, DataType.JSON) //对于表格的单元格来说, 这个类型是对的
            case JsonNodeType.NULL | JsonNodeType.MISSING => DataCell.NULL
            case JsonNodeType.NUMBER =>
                    if (node.isIntegralNumber) {
                        if (node.isLong) {
                            DataCell(node.longValue(), DataType.INTEGER)
                        }
                        else {
                            DataCell(node.intValue(), DataType.INTEGER)
                        }
                    }
                    else {
                        if (node.isFloat) {
                            DataCell(node.floatValue(), DataType.DECIMAL)
                        }
                        else {
                            DataCell(node.doubleValue(), DataType.DECIMAL)
                        }
                    }
            case JsonNodeType.BOOLEAN => DataCell(node.booleanValue(), DataType.BOOLEAN)
            case _ => DataCell(node.toString, DataType.TEXT)
        }
    }
    
    /* Can work at cluster mode. e.g. hadoop jar keeper.jar
    def findNode(path: String): JsonNode = {
        root.at(if (path.endsWith("/")) path.dropRight(1) else path)
    } */
    
    def findNode(path: String): JsonNode = {
        var p = path
        if (p.startsWith("/")) p = p.substring(1)
        
        var node: JsonNode = root
        while (node != null && !node.isNull && !p.isEmpty) {
            
            val section = if (p.contains("/")) p.substring(0, p.indexOf("/")).trim else p.trim
            p = if (p.contains("/")) p.substring(p.indexOf("/") + 1) else ""
            
            if (!section.isEmpty) {
                if (node.isArray) {
                    node = Try(section.toInt) match {
                        case Success(v) => node.get(v)
                        case Failure(_) => node.get(0)
                    }
                }
                else if (node.isObject) {
                    node = node.get(section)
                }
            }
        }
        
        node
    }

    def parseNode(path: String): Any = {
        val node = findNode(path)
        if (node.isArray) {
            val list: java.util.List[Any] = new java.util.ArrayList[Any]()
            node.elements().forEachRemaining(child => {
                if (child.isValueNode) {
                    if (child.isIntegralNumber || child.isInt || child.isShort) {
                        list.add(child.intValue())
                    }
                    else if (child.isLong || child.isBigInteger) {
                        list.add(child.longValue())
                    }
                    else if (child.isFloatingPointNumber || child.isFloat) {
                        list.add(child.floatValue())
                    }
                    else if (child.isDouble || child.isBigDecimal) {
                        list.add(child.doubleValue())
                    }
                    else if (child.isBoolean) {
                        list.add(child.booleanValue())
                    }
                    else if (node.isNull) {
                        list.add(null)
                    }
                    else {
                        list.add(child.textValue())
                    }
                }
                else {
                    val map = new java.util.HashMap[String, Any]()
                    child.fields().forEachRemaining(item => {
                        map.put(item.getKey, getCell(item.getValue))
                    })
                    list.add(map)
                }
            })
            list
        }
        else if (node.isObject) {
            val map = new java.util.HashMap[String, Any]()
            node.fields().forEachRemaining(item => {
                map.put(item.getKey, getCell(item.getValue))
            })
            map
        }
        else {
            if (node.isIntegralNumber || node.isInt || node.isShort) {
                node.intValue()
            }
            else if (node.isLong || node.isBigInteger) {
                node.longValue()
            }
            else if (node.isFloatingPointNumber || node.isFloat) {
                node.floatValue()
            }
            else if (node.isDouble || node.isBigDecimal) {
                node.doubleValue()
            }
            else if (node.isBoolean) {
                node.booleanValue()
            }
            else if (node.isNull) {
                null
            }
            else {
                node.textValue()
            }
        }
    }

//    def asObject(): java.util.Map[String, Any] = {
//
//    }
//
//    def asArray(): java.util.List[Any] = {
//        null
//    }

    override def toString: String = {
        root.toString
    }
}
