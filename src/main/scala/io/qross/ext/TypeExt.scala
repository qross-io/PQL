package io.qross.ext

import java.sql.{Date, Time, Timestamp}

import io.qross.core.DataType.DataType
import io.qross.core._
import io.qross.net.Json
import io.qross.setting.Global
import io.qross.sql.SQLExecuteException
import io.qross.time.{DateTime, Timer}
import javax.script.{ScriptEngine, ScriptEngineManager, ScriptException}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.sys.process._
import scala.util.control.Breaks._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object TypeExt {

    implicit class StringExt(var string: String) {

        def toBooleanOrElse(defaultValue: Boolean = false): Boolean = {
            if (Set("yes", "true", "1", "on", "ok").contains(string)) {
                true
            }
            else if (Set("no", "false", "0", "off", "cancel").contains(string)) {
                false
            }
            else {
                defaultValue
            }
        }

        def toQrossPath: String = {
            string.replace("\\", "/")
                    .replace("%USER_HOME", Global.USER_HOME)
                    .replace("%JAVA_BIN_HOME", Global.JAVA_BIN_HOME)
                    .replace("%QROSS_HOME", Global.QROSS_HOME)
                    .replace("%QROSS_KEEPER_HOME", Global.QROSS_KEEPER_HOME)
                    .replace("%QROSS_WORKER_HOME", Global.QROSS_WORKER_HOME)
                    .replace("//", "/")
        }

        def toByteLength: Long = {
            var number = string.dropRight(1)
            var times: Long = string.takeRight(1).toUpperCase() match {
                case "B" => 1
                case "K" => 1024
                case "M" => 1024 * 1024
                case "G" => 1024 * 1024 * 1024
                case "T" => 1024 * 1024 * 1024 * 1024
                case "P" => 1024 * 1024 * 1024 * 1024 * 1024
                case b =>
                    Try(b.toInt) match {
                        case Success(n) => number += n
                        case _ =>
                    }
                    1
            }

            Try(number.toLong) match {
                case Success(n) => n * times
                case _ => 0
            }
        }

        def toHashMap(delimiter: String = "&", terminator: String = "="): Map[String, String] = {
            val params = string.split(delimiter)
            val queries = new mutable.HashMap[String, String]()
            for (param <- params) {
                if (param.contains(terminator)) {
                    queries += param.substring(0, param.indexOf(terminator)) -> param.substring(param.indexOf(terminator) + 1)
                }
                else {
                    queries += param -> ""
                }
            }
            queries.toMap
        }

        //执行javascript
        def eval(): DataCell = {
            if (string.bracketsWith("{", "}")) {
                DataCell(Json(string).parseRow("/"), DataType.ROW)
            }
            else if (string.bracketsWith("[", "]")) {
                val table = Json(string).parseTable("/")
                if (table.columnCount == 1 && table.getFieldNames.head == "_array") {
                    DataCell(table.toJavaList, DataType.ARRAY)
                }
                else {
                    DataCell(table, DataType.TABLE)
                }
            }
            else {
                val jse: ScriptEngine = new ScriptEngineManager().getEngineByName("JavaScript")
                try {
                    DataCell(jse.eval(string))
                }
                catch {
                    case e: ScriptException =>
                        e.printStackTrace()
                        //DataCell(null)
                        throw new SQLExecuteException("Can't caculate expression: " + string)
                }
            }

            //The code below doesn't work.
            //import scala.tools.nsc._
            //val interpreter = new Interpreter(new Settings())
            //interpreter.interpret("import java.text.{SimpleDateFormat => sdf}")
            //interpreter.bind("date", "java.util.Date", new java.util.Date());
            //interpreter.eval[String]("""new sdf("yyyy-MM-dd").format(date)""") get
            //    interpreter.close()
        }

        //执行javascript并返回值, 最后一条语句需以return结尾
        def call(): DataCell = {
            val jse: ScriptEngine = new ScriptEngineManager().getEngineByName("JavaScript")
            try {
                DataCell(jse.eval(s"""(function(){ $string })()"""))
            }
            catch {
                case e: ScriptException =>
                    e.printStackTrace()
                    throw new SQLExecuteException("Can't caculate expression: " + string)
                //DataCell(null)
            }
        }

        //去掉变量修饰符
        def removeVariableModifier(): String = {
            string.replace("$", "").replace("{", "").replace("}", "").trim
        }

        //为计算值添加引号
        def useQuotes(quote: String): String = {
            if (quote == "\"") {
                "\"" + string.replace("\"", "\\\"") + "\""
            }
            else {
                "'" + string.replace("'", "\\'") + "'"
            }
        }

        def userQuotesIf(quote: String, condition: Boolean): String = {
            if (condition) {
                string.useQuotes(quote)
            }
            else {
                string
            }
        }

        //去掉常量中的双引号，用于PSQL计算结果
        def removeQuotes(): String = {
            if (string.bracketsWith("\"", "\"") || string.bracketsWith("'", "'")) {
                string.substring(1, string.length - 1).replace("\\\"", "\"")
            }
            else {
                string
            }
        }

        def $trim(prefix: String, suffix: String = "NULL"): String = {
            string = string.trim
            if (string.startsWith(prefix)) {
                string = string.drop(prefix.length)
            }
            val sfx = if (suffix == "NULL") prefix else suffix
            if (string.endsWith(sfx)) {
                string = string.dropRight(sfx.length)
            }
            string
        }

        def $quote(char: String): String = {
            if (!string.startsWith(char)) {
                string = char + string
            }
            else if (!string.endsWith(char)) {
                string += char
            }

            string
        }

        def takeBefore(value: Any): String = {
            value match {
                case char: String => string.take(string.indexOf(char))
                case index: Integer => string.take(index)
                case rex: Regex => string.take(string.indexOf(rex.findFirstIn(string).getOrElse("")))
                case _ => ""
            }
        }

        def takeAfter(value: Any): String = {
            value match {
                case char: String =>
                    if (char == "" || !string.contains(char)) {
                        string
                    }
                    else {
                        string.substring(string.indexOf(char) + char.length)
                    }
                case index: Integer => string.substring(index + 1)
                case rex: Regex =>
                    rex.findFirstIn(string) match {
                        case Some(v) => string.substring(string.indexOf(v) + v.length)
                        case None => string
                    }
                case _ => ""
            }
        }

        def takeRightAfter(value: Any): String = {
            value match {
                case char: String =>
                    if (char == "" || !string.contains(char)) {
                        ""
                    }
                    else {
                        string.substring(string.lastIndexOf(char) + char.length)
                    }
                case index: Integer => string.substring(index + 1)
                case rex: Regex =>
                    rex.findAllIn(string).toList.lastOption match {
                        case Some(v) => string.substring(string.lastIndexOf(v) + v.length)
                        case None => ""
                    }
                case _ => ""
            }
        }

        def takeBetween(left: Any, right: Any): String = {
            val li: Int = left match {
                case char: String => string.indexOf(char) + char.length
                case index: Int => index
                case rex: Regex =>
                    rex.findFirstIn(string) match {
                        case Some(v) => string.indexOf(v) + v.length
                        case None => 0
                    }
                case _ => 0
            }

            val ri: Int = right match {
                case char: String => string.lastIndexOf(char)
                case index: Int => index
                case rex: Regex =>
                    rex.findAllIn(string).toList.lastOption match {
                        case Some(v) => string.lastIndexOf(v)
                        case None => string.length - 1
                    }
                case _ => string.length - 1
            }

            string.substring(li, ri)
        }

        def bracketsWith(left: String, right: String): Boolean = {
            string.startsWith(left) && string.endsWith(right)
        }

        def $startsWith(strings: String*): Boolean = {
            var found = false
            breakable {
                for (str <- strings) {
                    if (string.startsWith(str)) {
                        found = true
                        break
                    }
                }
            }
            found
        }


        def ifEmpty(defaultValue: String): String = {
            if (string == "") {
                defaultValue
            }
            else {
                string
            }
        }

        def ifNull(defaultValue: String): String = {
            if (string == null) {
                defaultValue
            }
            else {
                string
            }
        }

        def ifNullOrEmpty(defaultValue: String): String = {
            if (string == null || string == "") {
                defaultValue
            }
            else {
                string
            }
        }

        def bash(): Int = {
            val exitValue = string.!(ProcessLogger(out => {
                println(out)
            }, err => {
                System.err.println(err)
                //println(err)
            }))

            exitValue
        }

        def go(): Int = {

            val process = string.run(ProcessLogger(out => {
                println(out)
            }, err => {
                System.err.println(err)
            }))

            var i = 0
            while(process.isAlive()) {
                println("s#" + i)
                i += 1
                //            if (i > 10) {
                //                process.destroy()
                //            }
                Timer.sleep(1)
            }

            //println("exitValue: " + process.exitValue())
            //process.destroy()

            process.exitValue()
        }
    }

    implicit class LongExt(long: Long) {
        def toHumanized: String = {
            var size: Double = long
            val units = List("B", "K", "M", "G", "T", "P")
            var i = 0
            while (size > 1024 && i < 6) {
                size /= 1024
                i += 1
            }

            f"$size%.2f${units(i)}"
        }

        def toDateTime: DateTime = {
            DateTime.of(long)
        }
    }

    implicit class FloatExt(float: Float) {
        def floor(precision: Int = 0): Double = {
            Math.floor(float * Math.pow(10, precision)) / Math.pow(10, precision)
        }

        def round(precision: Int = 0): Double = {
            Math.round(float * Math.pow(10, precision)) / Math.pow(10, precision)
        }
    }

    implicit class DoubleExt(double: Double) {
        def floor(precision: Int = 0): Double = {
            Math.floor(double * Math.pow(10, precision)) / Math.pow(10, precision)
        }

        def round(precision: Int = 0): Double = {
            Math.round(double * Math.pow(10, precision)) / Math.pow(10, precision)
        }
    }

    implicit class RegexExt(regex: Regex) {
        def test(str: String): Boolean = {
            regex.findFirstIn(str).nonEmpty
        }
    }

    //for Sharp Expression
    implicit class AnyExt(any: Any) {

        def print: Any = {
            println(any)
            any
        }

        def toText: String = {
            any match {
                case str: String => str
                case _ => any.toString
            }
        }

        def toInteger: Long = {
            any match {
                case str: String =>
                    Try(str.toLong) match {
                        case Success(l) => l
                        case Failure(_) => throw new ConvertFailureException("Can't recognize as or convert to Integer: " + any)
                    }
                case dt: DateTime => dt.toEpochSecond
                case i: Int => i
                case l: Long => l
                case f: Float => Math.round(f)
                case d: Double => Math.round(d)
                case b: Boolean => if (b) 1 else 0
                case _ => throw new ConvertFailureException("Can't recognize as or convert to Integer: " + any)
            }
        }

        def toInteger(defaultValue: Any): Long = {
            try {
                any.toInteger
            }
            catch {
                case _ : ConvertFailureException => defaultValue.toInteger
            }
        }

        def toDecimal: Double = {
            any match {
                case str: String =>
                    Try(str.toDouble) match {
                        case Success(d) => d
                        case Failure(_) => throw new ConvertFailureException("Can't recognize as or convert to Decimal: " + any)
                    }
                case dt: DateTime => dt.toEpochSecond
                case i: Int => i
                case l: Long => l
                case f: Float => f
                case d: Double => d
                case b: Boolean => if (b) 1 else 0
                case _ => throw new ConvertFailureException("Can't recognize as or convert to Decimal: " + any)
            }
        }

        def toDecimal(defaultValue: Any): Double = {
            try {
                any.toDecimal
            }
            catch {
                case _ : ConvertFailureException => defaultValue.toDecimal
            }
        }

        def toDateTime: DateTime = {
            any match {
                case str: String => DateTime(str.removeQuotes())
                case dt: DateTime => dt
                case date: Date => DateTime(date.toString)
                case time: Time => DateTime(time.toString)
                case ts: Timestamp => DateTime(ts.toString)
                case l: Long => DateTime.of(l)
                case i: Int => DateTime.of(i)
                case _ => throw new ConvertFailureException("Can't recognize as or convert to DateTime: " + any)
            }
        }

        def toDateTime(defaultValue: Any): DateTime = {
            try {
                any.toDateTime
            }
            catch {
                case _ : ConvertFailureException => defaultValue.toDateTime
            }
        }

        def toBoolean: Boolean = {
            any match {
                case str: String =>
                    if (Set("yes", "true", "1", "on", "ok").contains(str.toLowerCase())) {
                        true
                    }
                    else if (Set("no", "false", "0", "off", "cancel").contains(str.toLowerCase())) {
                        false
                    }
                    else {
                        throw new ConvertFailureException("Can't recognize as or convert to Boolean: " + any)
                    }
                case b: Boolean => b
                case i: Int => i == 1
                case l: Long => l == 1l
                case d: Double => d == 1d
                case f: Float => f == 1f
                case _ => throw new ConvertFailureException("Can't recognize as or convert to Boolean: " + any)
            }
        }

        def toBoolean(defaultValue: Any): Boolean = {
            try {
                any.toBoolean
            }
            catch {
                case _ : ConvertFailureException => defaultValue.toBoolean
            }
        }

        def toTable: DataTable = {
            any match {
                case dt: DataTable => dt
                case row: DataRow => row.toTable()
                case JavaList(list) => list.asScala.toList.toTable()
                case JavaMap(map) => map.asScala.toMap.toTable()
                case ScalaList(list) => list.toTable()
                case ScalaMap(map) => map.toTable()
                case str: String => str.toJson.parseTable("/")
                case other => DataTable(DataRow("value" -> other))
            }
        }

        def toJavaList: java.util.List[Any] = {
            any match {
                case str: String =>
                        //不能直接转换, 类型不匹配
                        val list = new mutable.ListBuffer[Any]()
                        str.split(",", -1).foreach(s => list += s)
                        list.asJava
                case JavaList(list) => list
                case JavaMap(map) => map.asScala.values.toList.asJava
                case ScalaList(list) => list.asJava
                case ScalaMap(map) => map.values.toList.asJava
                case dt: DataTable => dt.toJavaList
                case row: DataRow => row.toJavaList
                case other => List(other).asJava
            }
        }

        def toScalaList: List[Any] = {
            any match {
                case str: String => str.split(",", -1).toList
                case JavaList(list) => list.asScala.toList
                case JavaMap(map) => map.asScala.values.toList
                case ScalaList(list) => list
                case ScalaMap(map) => map.values.toList
                case dt: DataTable => dt.toJavaList.asScala.toList
                case row: DataRow => row.toJavaList.asScala.toList
                case other => List(other)
            }
        }

        def toJson: Json = {
            any match {
                case str: String =>
                    if (str.startsWith("http://") || str.startsWith("https://")) {
                        Json.fromURL(str)
                    }
                    else {
                        Json.fromText(str.removeQuotes())
                    }
                case json: Json => json
                case _ => throw new ConvertFailureException("Can't recognize as or convert to Json: " + any)
            }
        }

        def toDataCell: DataCell = {
            DataCell(any)
        }

        def toDataCell(dataType: DataType): DataCell = {
            DataCell(any, dataType)
        }
    }

    implicit class ListExt[A](list: List[A]) {
        def toTable(fieldName: String = "item"): DataTable = {
            val table = new DataTable()
            for (item <- list) {
                item match {
                    case ScalaMap(map) => table.addRow(map.toRow)
                    case JavaMap(map) => table.addRow(map.asScala.toMap.toRow)
                    case array: List[Any] =>
                        val row = new DataRow()
                        for (i <- array.indices) {
                            row.set(fieldName + i, array(i))
                        }
                        table.addRow(row)
                    case _ => table.addRow(DataRow(fieldName -> item))
                }
            }
            table
        }
    }

    implicit class MapExt[A, B](map: Map[A, B]) {
        def toTable(keyName: String = "key", valueName: String = "value"): DataTable = {
            val table = new DataTable()
            for (item <- map) {
                table.addRow(DataRow(keyName -> item._1, valueName -> item._2))
            }
            table
        }

        def toRow: DataRow = {
            val row = new DataRow()
            for (item <- map) {
                row.set(item._1.toString, item._2)
            }
            row
        }
    }
}
