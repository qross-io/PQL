package io.qross.ext

import java.util
import java.util.regex.Pattern

import io.qross.core._
import io.qross.exception.{ConvertFailureException, SQLExecuteException}
import io.qross.net.Json
import io.qross.setting.Global
import io.qross.time.{DateTime, Timer}
import io.qross.pql.Solver._
import javax.script.{ScriptEngine, ScriptEngineManager, ScriptException}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.sys.process._
import scala.util.control.Breaks._
import scala.util.matching.Regex
import scala.util.{Failure, Random, Success, Try}

object TypeExt {

    var $m: Regex.Match = _

    implicit class StringExt(var string: String) {

        def toBoolean(defaultValue: Boolean): Boolean = {
            string = string.toLowerCase()
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
//                    .replace("%QROSS_KEEPER_HOME", Global.QROSS_KEEPER_HOME)
//                    .replace("%QROSS_WORKER_HOME", Global.QROSS_WORKER_HOME)
                    .replace("//", "/")
        }

        def toByteLength: Long = {
            var number = string.dropRight(1)
            val times: Long = string.takeRight(1).toUpperCase() match {
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

        def $split(delimiter: String = "&", terminator: String = "="): Map[String, String] = {
            val params = string.split(delimiter)
            val queries = new mutable.LinkedHashMap[String, String]()
            for (param <- params) {
                if (param.contains(terminator)) {
                    queries += param.takeBefore(terminator) -> param.takeAfter(terminator)
                }
                else {
                    queries += param -> ""
                }
            }
            queries.toMap
        }

        def evaluate(): Any = {
            val jse: ScriptEngine = new ScriptEngineManager().getEngineByName("JavaScript")
            try {
                jse.eval(string)
            }
            catch {
                case e: ScriptException =>
                    e.printStackTrace()
                    throw new SQLExecuteException("Can't calculate expression: " + string)
                    DataCell.ERROR
            }
        }

        //执行javascript
        def eval(): DataCell = {
            if (string.bracketsWith("{", "}")) {
                DataCell(Json(string).parseRow("/"), DataType.ROW)
            }
            else if (Json.OBJECT$ARRAY.test(string)) {
                DataCell(Json(string).parseTable("/"), DataType.TABLE)
            }
            else if (string.bracketsWith("[", "]")) {
                DataCell(Json(string).parseJavaList("/"), DataType.ARRAY)
            }
            else if (string.isJsRegex) {
                DataCell(string.toRegex, DataType.REGEX)
            }
            else {
                val jse: ScriptEngine = new ScriptEngineManager().getEngineByName("JavaScript")
                try {
                    val data = DataCell(jse.eval(string))
                    if (data.isText) {
                        data.value = data.value.asInstanceOf[String].restoreSymbols()
                    }
                    data
                }
                catch {
                    case e: ScriptException =>
                        e.printStackTrace()
                        throw new SQLExecuteException("Can't calculate expression: " + string)
                        DataCell.ERROR
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

        def quotesWith(quote: String): Boolean = {
            string.bracketsWith(quote, quote)
        }

        //为计算值添加引号
        def useQuotes(quote: String): String = {
            if (quote == "\"") {
                "\"" + string.replace("\"", "\\\"") + "\""
            }
            else if (quote == "'") {
                "'" + string.replace("'", "\\'") + "'"
            }
            else if (quote != "") {
                quote + string + quote
            }
            else {
                string
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

        //去掉常量中的双引号，用于PQL计算结果
        def removeQuotes(): String = {
            if (string.bracketsWith("\"", "\"")) {
                string.substring(1, string.length - 1).replace("\\\"", "\"")
            }
            else if (string.bracketsWith("'", "'")) {
                string.substring(1, string.length - 1).replace("\\'", "'")
            }
            else if (string.bracketsWith("`", "`")) {
                string.$trim("`", "`")
            }
            else {
                string
            }
        }

        def $trim(prefixOrSuffix: String): String = {
            string = string.trim
            while (string.startsWith(prefixOrSuffix)) {
                string = string.drop(prefixOrSuffix.length)
            }
            while (string.endsWith(prefixOrSuffix)) {
                string = string.dropRight(prefixOrSuffix.length)
            }
            string
        }

        def $trim(prefix: String, suffix: String): String = {
            string = string.trim()
            if (string.startsWith(prefix)) {
                string = string.drop(prefix.length)
            }
            if (string.endsWith(suffix)) {
                string = string.dropRight(suffix.length)
            }
            string
        }

        def $trimLeft(prefix: String = ""): String = {
            string = string.drop("^\\s*".r.findFirstIn(string).getOrElse("").length)
            if (prefix != "" && string.startsWith(prefix)) {
                string = string.drop(prefix.length)
            }
            string
        }

        def $trimRight(suffix: String = ""): String = {
            string = string.dropRight("\\s*$".r.findFirstIn(string).getOrElse("").length)
            if (suffix != "" && string.endsWith(suffix)) {
                string = string.dropRight(suffix.length)
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
                case _ => string
            }
        }

        def takeBeforeLast(value: Any): String = {
            value match {
                case char: String =>
                    if (char == "" || !string.contains(char)) {
                        ""
                    }
                    else {
                        string.substring(0, string.lastIndexOf(char))
                    }
                case index: Integer => string.substring(0, index)
                case rex: Regex =>
                    rex.findAllIn(string).toList.lastOption match {
                        case Some(v) => string.substring(0, string.lastIndexOf(v))
                        case None => ""
                    }
                case _ => ""
            }
        }

        def takeAfterLast(value: Any): String = {
            value match {
                case char: String =>
                    if (char == "" || !string.contains(char)) {
                        string
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
                case _ => string
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

            if (ri > li) {
                string.substring(li, ri)
            }
            else if (ri == li) {
                string.substring(li)
            }
            else {
                string.substring(ri, li)
            }
        }

        def bracketsWith(left: String, right: String): Boolean = {
            string.startsWith(left) && string.endsWith(right)
        }

        def bracket(side: String): String = {
            side + string + side
        }

        def bracket(left: String, right: String): String = {
            left + string + right
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

        def initCap: String = string.take(1).toUpperCase + string.substring(1)

        def preventInjection: String = string.replace("'", "~u0027~u0027").replace("\\", "\\\\")
        def preventInjectionOfDoubleQuote: String = string.replace("\"", "~u0022~u0022").replace("\\", "\\\\")

        def isJsRegex: Boolean = string.startsWith("/") && string.indexOf("/") < string.lastIndexOf("/") && "/[ig]*$".r.test(string)

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

            if (string.contains("|")) {
                val cmd = string.split("\\|")
                var sh = cmd(0).trim() #| cmd(1).trim()
                for (i <- 2 until cmd.length) {
                    sh = sh #| cmd(i).trim()
                }

                sh.!(
                    ProcessLogger(out => {
                        println(out)
                    }, err => {
                        System.err.println(err)
                    })
                )
            }
            else {
                string.!(
                    ProcessLogger(out => {
                        println(out)
                    }, err => {
                        System.err.println(err)
                    })
                )
            }
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
                Timer.sleep(1000)
            }

            //println("exitValue: " + process.exitValue())
            //process.destroy()

            process.exitValue()
        }

        def pickChars(): String = {
            pickChars(new ListBuffer[String]())
        }

        //处理字符串
        def pickChars(chars: ListBuffer[String]): String = {

            if (string.contains("'") || string.contains("\"")) {
                var pre = ' '
                var pri = 0
                for (i <- string.indices) {
                    val c = string.charAt(i)
                    if (c == '\'') {
                        if (pre == ' ') {
                            pre = '\''
                            pri = i
                        }
                        else if (pre == '\'' && (if (i > 0) string.charAt(i - 1) != '\\' else true)) {
                            chars += string.substring(pri, i + 1)
                            pre = ' '
                        }
                    }
                    else if (c == '"') {
                        if (pre == ' ') {
                            pre = '"'
                            pri = i
                        }
                        else if (pre == '"' && (if (i > 0) string.charAt(i - 1) != '\\' else true)) {
                            chars += string.substring(pri, i + 1)
                            pre = ' '
                        }
                    }
                }

                for (i <- chars.indices) {
                    string = string.takeBefore(chars(i)) + s"~str[$i]" + string.takeAfter(chars(i))
                }
            }

            string
        }

        def restoreChars(chars: ListBuffer[String]): String = {
            """~str\[(\d+)\]""".r
                .findAllMatchIn(string)
                .foreach(m => {
                    val i = m.group(1).toInt
                    if (i < chars.size) {
                        string = string.replace(m.group(0), chars(i))
                    }
                })

            string
        }

        def indexPairOf(left: Char, right: Char): (Int, Int) = {
            var s = 0 //stack
            var l = -1
            var r = -1
            breakable {
                for (i <- string.indices) {
                    val c = string.charAt(i)
                    if (c == left) {
                        s += 1
                        if (l == -1) {
                            l = i
                        }
                    }
                    else if (c == right) {
                        s -= 1
                        if (s == 0) {
                            r = i
                            break
                        }
                    }
                }
            }

            (l, r)
        }

        def shuffle(digit: Int = 1): String = {
            val length = string.length
            val sb = new mutable.StringBuilder()
            for (i <- 1 to digit) {
                val random = Random.nextInt(length)
                sb.append(string.substring(random, random + 1))
            }
            sb.toString()
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
            DateTime.ofTimestamp(long)
        }
    }

    implicit class FloatExt(float: Float) {
        def floor(precision: Int = 0): Double = {
            Math.floor(float * Math.pow(10, precision)) / Math.pow(10, precision)
        }

        def round(precision: Int = 0): Double = {
            Math.round(float * Math.pow(10, precision)) / Math.pow(10, precision)
        }

        def percent: String = {
            s"${float * 100}%"
        }

        def percent(precision: Int = 0): String = {
            s"${Math.round(float * 100 * Math.pow(10, precision)) / Math.pow(10, precision)}%"
        }

        def pow(p: Double = 2): Double = Math.pow(float, p)
    }

    implicit class DoubleExt(double: Double) {
        def floor(precision: Int = 0): Double = {
            Math.floor(double * Math.pow(10, precision)) / Math.pow(10, precision)
        }

        def round(precision: Int = 0): Double = {
            Math.round(double * Math.pow(10, precision)) / Math.pow(10, precision)
        }

        def percent: String = {
            s"${double * 100}%"
        }

        def percent(precision: Int = 0): String = {
            s"${Math.round(double * 100 * Math.pow(10, precision)) / Math.pow(10, precision)}%"
        }

        def pow(p: Double = 2): Double = Math.pow(double, p)
    }

    implicit class RegexExt(regex: Regex) {
        def test(str: String): Boolean = {
            regex.findFirstIn(str).nonEmpty
        }

        def matches(str: String): Boolean = {
            //将最后一次匹配的结果保存到全局变量$m
            regex.findFirstMatchIn(str) match {
                case Some(m) => $m = m; true
                case None => $m = null; false
            }
        }

        def exec(str: String): Array[String] = {
            regex.findFirstMatchIn(str) match {
                case Some(m) =>
                    val result = new mutable.ArrayBuffer[String]()
                    for (i <- 0 to m.groupCount) {
                        result += m.group(i)
                    }
                    result.toArray
                case None => new Array[String](0)
            }
        }
    }

    //for Sharp Expression
    implicit class AnyExt(any: Any) {

        //打印并返回该对象
        def print[T]: T = {
            println(any)
            any.asInstanceOf[T]
        }

        def printMore[T](prefix: String): T = {
            Predef.print(prefix)
            Predef.println(any)
            any.asInstanceOf[T]
        }

        def printMore[T](prefix: String, suffix: String): T = {
            Predef.print(prefix)
            Predef.print(any)
            Predef.println(suffix)
            any.asInstanceOf[T]
        }

        def toText: String = {
            any match {
                case str: String => str
                case rex: Regex => rex.regex
                case null => null
                case _ => any.toString
            }
        }

        def toRegex: Regex = {
            any match {
                case rex: Regex => rex
                case pattern: Pattern => {
                        if (pattern.flags() == 2) {
                            "(?i)"
                        }
                        else {
                            ""
                        } + pattern.pattern()
                    }.r
                case str: String => {
                    if (str.isJsRegex) {
                        if (str.takeAfterLast("/")
                            .toLowerCase()
                            .contains("i")) {
                            "(?i)" + str.takeBetween("/", "/")
                        }
                        else {
                            str.takeBetween("/", "/")
                        }
                    }
                    else {
                        str
                    }
                }.r
                case x => x.toString.r
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
                case bi: java.math.BigInteger => bi.longValue()
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
                case dt: DateTime => dt.toEpochMilli
                case i: Int => i
                case l: Long => l
                case f: Float => f
                case d: Double => d
                case b: Boolean => if (b) 1 else 0
                case bd: java.math.BigDecimal => bd.stripTrailingZeros().doubleValue()
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
            new DateTime(any)
        }

        def toDateTime(format: String): DateTime = {
            new DateTime(any, format)
        }

        def toDateTimeOrElse(defaultValue: DateTime): DateTime = {
            try {
                new DateTime(any)
            }
            catch {
                case _: Exception => defaultValue
            }
        }

        def toDateTimeOrElse(format: String, defaultValue: DateTime): DateTime = {
            try {
                new DateTime(any, format)
            }
            catch {
                case _: Exception => defaultValue
            }
        }

        def toBoolean: Boolean = {
            any match {
                case str: String =>
                    if (Set("yes", "true", "1", "on", "ok", "y").contains(str.toLowerCase())) {
                        true
                    }
                    else if (Set("no", "false", "0", "off", "cancel", "n").contains(str.toLowerCase())) {
                        false
                    }
                    else {
                        throw new ConvertFailureException("Can't recognize as or convert to Boolean: " + any)
                    }
                case b: Boolean => b
                case i: Int => i > 0
                case l: Long => l > 0l
                case f: Float => f > 0f
                case d: Double => d > 0d
                case cell: DataCell => cell.asBoolean(false)
                case dt: DataTable => dt.nonEmpty
                case row: DataRow => row.nonEmpty
                case list: java.util.List[Any@unchecked] => !list.isEmpty
                case null => false
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

        def toJson: Json = {
            any match {
                case json: Json => json
                case str: String => {
                    if (str.bracketsWith("{", "}") || str.bracketsWith("[", "]")) {
                        Json(str)
                    }
                    else if (str.startsWith("http://") || str.startsWith("https://")) {
                        Json.fromURL(str)
                    }
                    else {
                        Json(str.useQuotes("\""))
                    }
                }
                case dt: DataTable => Json(dt.toString)
                case row: DataRow => Json(row.toString)
                case l: Long => Json(l.toString)
                case i: Int => Json(i.toString)
                case f: Float => Json(f.toString)
                case d: Double => Json(d.toString)
                case o: AnyRef => Json(Json.serialize(o))
                case _ => Json()
            }
        }

        def toDataCell: DataCell = {
            DataCell(any)
        }

        def toDataCell(dataType: DataType): DataCell = {
            DataCell(any, dataType)
        }

        def >=(other: Any): Boolean = {
            any match {
                case dt: DateTime => dt.afterOrEquals(other.toDateTime)
                case i: Int => i >= other.toInteger(0)
                case l: Long => l >= other.toInteger(0)
                case f: Float => f >= other.toDecimal(0)
                case d: Double => d >= other.toDecimal(0)
                case _ => any.toString.compareToIgnoreCase(other.toString) >= 0
            }
        }

        def <=(other: Any): Boolean = {
            any match {
                case dt: DateTime => dt.beforeOrEquals(other.toDateTime)
                case i: Int => i <= other.toInteger(0)
                case l: Long => l <= other.toInteger(0)
                case f: Float => f <= other.toDecimal(0)
                case d: Double => d <= other.toDecimal(0)
                case _ => any.toString.compareToIgnoreCase(other.toString) <= 0
            }
        }

        def >(other: Any): Boolean = {
            any match {
                case dt: DateTime => dt.after(other.toDateTime)
                case i: Int => i > other.toInteger(0)
                case l: Long => l > other.toInteger(0)
                case f: Float => f > other.toDecimal(0)
                case d: Double => d > other.toDecimal(0)
                case _ => any.toString.compareToIgnoreCase(other.toString) > 0
            }
        }

        def <(other: Any): Boolean = {
            any match {
                case dt: DateTime => dt.before(other.toDateTime)
                case i: Int => i < other.toInteger(0)
                case l: Long => l < other.toInteger(0)
                case f: Float => f < other.toDecimal(0)
                case d: Double => d < other.toDecimal(0)
                case _ => any.toString.compareToIgnoreCase(other.toString) < 0
            }
        }
    }

    implicit class ListExt[A](list: List[A]) {

        def toJavaList: java.util.List[A] = {
            val column = new util.ArrayList[A]()
            list.foreach(column.add)
            column
        }

        def toJsonString: String = {
            Json.serialize(list)
        }

        def delimit(delimiter: String): Map[String, String] = {
            list.map(v => {
                (v.asInstanceOf[String].takeBefore(delimiter), v.asInstanceOf[String].takeAfter(delimiter))
            }).toMap
        }
    }

    implicit class MapExt[A, B](map: Map[A, B]) {

        def toRow: DataRow = {
            val row = new DataRow()
            for (item <- map) {
                row.set(item._1.toString, item._2)
            }
            row
        }

        def toJsonString: String = {
            Json.serialize(map)
        }
    }
}