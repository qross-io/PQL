package io.qross.pql

import java.util

import io.qross.core._
import io.qross.exception.{ExtensionNotFoundException, SQLParseException}
import io.qross.ext.TypeExt._
import io.qross.fs.SourceFile
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.net.Json
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
import io.qross.setting.{Configurations, Global, Language}
import io.qross.time.DateTime

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
 * PQL 单例对象，用于快速创建类或快速运行 PQL
 */
object PQL {

    //打开但不运行
    def open(SQL: String): PQL = {
        new PQL(SQL, new DataHub())
    }

    def openEmbedded(SQL: String): PQL = {
        new PQL(SQL, embedded = true, new DataHub())
    }

    def check(SQL: String): String = {
        PQL.open(SQL).check()
    }

    def checkPQL(SQL: String): String = {
        PQL.open(SQL).check()
    }

    def recognizeParametersIn(SQL: String): util.Set[String] = {
        PQL.open(SQL).recognizeParameters()
    }

    def recognizeParametersInEmbedded(SQL: String): util.Set[String] = {
        PQL.openEmbedded(SQL).recognizeParameters()
    }

    //直接运行
    def run(SQL: String): Any = {
        PQL.open(SQL).run()
    }

    def runPQL(SQL: String): Any = {
        PQL.open(SQL).run()
    }

    def runEmbedded(SQL: String): Any = {
        PQL.openEmbedded(SQL).run()
    }

    //打开文件但不运行
    def openFile(path: String): PQL = {
        new PQL(SourceFile.read(path), new DataHub())
    }

    def openFile(path: String, dh: DataHub): PQL = {
        new PQL(SourceFile.read(path), dh)
    }

    def openEmbeddedFile(path: String): PQL = {
        new PQL(SourceFile.read(path), embedded = true, new DataHub())
    }

    def openEmbeddedFile(path: String, dh: DataHub): PQL = {
        new PQL(SourceFile.read(path), embedded = true, dh)
    }

    def checkFile(path: String): String = {
        PQL.openFile(path).check()
    }

    def recognizeParametersInFile(path: String): util.Set[String] = {
        PQL.openFile(path).recognizeParameters()
    }

    def recognizeParametersInEmbeddedFile(path: String): util.Set[String] = {
        PQL.openEmbeddedFile(path).recognizeParameters()
    }

    //直接运行
    def runFile(path: String): Any = {
        PQL.openFile(path).run()
    }

    def runFile(path: String, dh: DataHub): Any = {
        PQL.openFile(path, dh).run()
    }

    def runEmbeddedFile(path: String): Any = {
        PQL.openEmbeddedFile(path).run()
    }

    def runEmbeddedFile(path: String, dh: DataHub): Any = {
        PQL.openEmbeddedFile(path, dh).run()
    }

    implicit class DataHub$PQL(private[pql] val dh: DataHub) {

        def PQL: PQL = {
            dh.pick[PQL]("PQL") match {
                case Some(pql) => pql
                case None => throw new ExtensionNotFoundException("Must use openSQL/openFileSQL/openResourceSQL method to open a PQL first.")
            }
        }

        def openPQL(SQL: String): DataHub = {
            dh.plug("PQL", new PQL(SQL, dh))
        }

        def openEmbeddedPQL(SQL: String): DataHub = {
            dh.plug("PQL", new PQL(SQL, embedded = true, dh))
        }

        def openFilePQL(filePath: String): DataHub = {
            dh.plug("PQL", new PQL(SourceFile.read(filePath), dh))
        }

        def openEmbeddedFilePQL(filePath: String): DataHub = {
            dh.plug("PQL", new PQL(SourceFile.read(filePath), embedded = true, dh))
        }

        def setArgs(name: String, value: String): DataHub = {
            PQL.place(name, value)
            dh
        }

        def setArgs(queryString: String): DataHub = {
            PQL.place(queryString)
            dh
        }

        def setArgs(args: (String, String)*): DataHub = {
            PQL.place(args: _*)
            dh
        }

        def setArgs(args: Map[String, String]): DataHub = {
            PQL.place(args)
            dh
        }

        def setVariable(name: String, value: Any): DataHub = {
            PQL.set(name, value)
            dh
        }

        def setVariables(queryString: String): DataHub = {
            PQL.set(queryString)
            dh
        }

        def setVariable(variables: (String, Any)*): DataHub = {
            PQL.set(variables: _*)
            dh
        }

        def setVariable(variables: DataRow): DataHub = {
            PQL.set(variables)
            dh
        }

        def run(): Any = {
            PQL.$run().$return
        }

        def run(SQL: String): Any = {
            new PQL(SQL, dh).$run().$return
        }

        def runEmbedded(SQL: String): Any = {
            new PQL(SQL, embedded = true, dh).$run().$return
        }

        def runFile(filePath: String): Any = {
            new PQL(SourceFile.read(filePath), dh).$run().$return
        }

        def runEmbeddedFile(filePath: String): Any = {
            new PQL(SourceFile.read(filePath), embedded = true, dh).$run().$return
        }
    }
}

/**
 * PQL 类，整个系统的核心，多个组件都是基于 PQL 实现或与 PQL 结合紧密
 * @param   originalSQL   原始 PQL 所有语句内容
 * @param   embedded      是否是嵌入式 PQL
 * @param   dh            用于运行 PQL 的 DataHub
  */
class PQL(val originalSQL: String, val embedded: Boolean, val dh: DataHub) {

    def this(originalSQL: String, dh: DataHub) {
        this(originalSQL, false, dh)
    }

    def this(originalSQL: String) {
        this(originalSQL, false, DataHub.DEFAULT)
    }

    def this(originalSQL: String, embedded: Boolean) {
        this(originalSQL, embedded, DataHub.DEFAULT)
    }

    //字符串 ~char[n]
    private[pql] val chars = new ArrayBuffer[String]()
    //富字符串 ~string[n]
    private[pql] val strings = new ArrayBuffer[String]()
    //Json常量 ~json[n]
    private[pql] val jsons = new ArrayBuffer[String]()
    //计算过程中的中间结果~value[n]
    private[pql] val values = new ArrayBuffer[DataCell]()
    //暂存的inner语句
    private[pql] val inners = new ArrayBuffer[String]()

    private[pql] var SQL: String = originalSQL

    private[pql] val root: Statement = new Statement("ROOT", SQL)

    //结果集
    private[pql] var BREAK: Boolean = false //RETURN 可触发全局中断
    private[pql] val RESULT: ArrayBuffer[Any] = new ArrayBuffer[Any]() //显式输出结果
    private[pql] val WORKING: ArrayBuffer[Any] = new ArrayBuffer[Any]() //隐式输出结果
    private[pql] var COUNT_OF_LAST_SELECT: Int = -1 //最后一个SELECT返回的结果数量
    private[pql] var AFFECTED_ROWS_OF_LAST_NON_QUERY: Int = -1  //最后一个非SELECT语句影响的数据表行数

    //正在解析的所有语句, 控制语句包含ELSE和ELSIF
    private[pql] val PARSING = new mutable.ArrayStack[Statement]()

    //正在执行的控制语句
    private[pql] val EXECUTING = new mutable.ArrayStack[Statement]()
    //待关闭的控制语句，如IF, FOR, WHILE等，不保存ELSE和ELSIF
    private[pql] val TO_BE_CLOSE = new mutable.ArrayStack[Statement]()
    //IF条件执行结果
    private[pql] lazy val IF$BRANCHES = new mutable.ArrayStack[Boolean]()
    //CASE语句对比结果
    private[pql] lazy val CASE$WHEN = new mutable.ArrayStack[Case$When]()
    //FOR语句循环项变量值
    private[pql] lazy val FOR$VARIABLES = new mutable.ArrayStack[ForVariables]()

    //用户定义的函数
    private[pql] lazy val USER$FUNCTIONS = new mutable.LinkedHashMap[String, UserFunction]()
    //函数的返回值
    private[pql] lazy val FUNCTION$RETURNS = new mutable.ArrayStack[DataCell]()

    private[pql] var breakCurrentLoop = false

    private[pql] val credential = new DataRow("userid" -> 0, "username" -> "anonymous", "role" -> "worker")

    //与 Keeper 相关的调度作业 id，用于保存仅作用于 job 范围的变量
    private[pql] var jobId = 0

    val ARGUMENTS = new DataRow()

    //开始解析
    private def parseAll(): Unit = {

        //check arguments
        //ARGUMENT.findAllMatchIn(SQL).foreach(m => Output.writeWarning(s"Argument ${m.group(0)} is not assigned."))

        //开始解析
        PARSING.push(root)

        val sentences: Array[String] = {
            if (!embedded) {
                SQL = SQL.cleanCommentsAndStashConstants(this)
                SQL.split(";").map(_.trim)
            }
            else {
                SQL.split(EM$RIGHT)
                    .flatMap(block => {
                        if (block.contains(EM$LEFT)) {
                            val s = new ArrayBuffer[String]()
                            val p1 = block.takeBefore(EM$LEFT)
                            if (p1.nonEmpty) {
                                s += "ECHO " + p1
                            }

                            val p2 = block.takeAfter(EM$LEFT)
                            if ("""^\s*=""".r.test(p2)) {
                                s += "OUTPUT " + p2.takeAfter("=").cleanCommentsAndStashConstants(this)
                            }
                            else {
                                s ++= p2.cleanCommentsAndStashConstants(this).split(";").map(_.trim)
                            }

                            s
                        }
                        else {
                            Array[String]("ECHO " + block)
                        }
                    })
            }
        }

        for (sentence <- sentences) {
            if (sentence.nonEmpty) {
                parseStatement(sentence)
            }
        }

        PARSING.pop()

        if (PARSING.nonEmpty || TO_BE_CLOSE.nonEmpty) {
            throw new SQLParseException("Control statement hasn't closed: \n" + {
                val sentence = TO_BE_CLOSE.head.sentence
                if (sentence.length > 120) {
                    sentence.substring(0, 120) + " ..."
                }
                else {
                    sentence
                }
            })
        }
    }

    //解析入口
    def parseStatement(sentence: String): Unit = {
        val caption: String = {
            if ("(?i)^[a-z]+#".r.test(sentence)) {
                sentence.takeBefore("#")
            }
            else if ($BLANK.test(sentence)) {
                sentence.takeBeforeX($BLANK).toUpperCase
            }
            else {
                sentence.toUpperCase
            }
        }
        if (NON_QUERY_CAPTIONS.contains(caption)) {
            PARSING.head.addStatement(new Statement(caption, sentence, new NON$QUERY(sentence)))
        }
        else {
            try {
                Class.forName(s"io.qross.pql.$caption")
                        .getDeclaredMethod("parse", classOf[String], classOf[PQL]) //Class.forName("io.qross.pql.PQL")
                        .invoke(null, sentence, this)
            }
            catch {
                case _: ClassNotFoundException => throw new SQLParseException("Unrecognized or unsupported sentence: " + sentence)
            }
        }
    }

    private[pql] def executeStatements(statements: ArrayBuffer[Statement]): Unit = {
        breakable {
            for (statement <- statements) {
                if (NON_QUERY_CAPTIONS.contains(statement.caption)) {
                    statement.instance.asInstanceOf[NON$QUERY].execute(this)
                }
                else if (statement.caption == "CONTINUE") {
                    if (CONTINUE.execute(this, statement)) {
                        break
                    }
                }
                else if (statement.caption == "EXIT") {
                    if (EXIT.execute(this, statement)) {
                        break
                    }
                }
                else if (statement.caption == "RETURN") {
                    statement.instance.asInstanceOf[RETURN].execute(this)
                    break
                }
                else if (STATEMENTS.contains(statement.caption)) {
                    Class.forName(s"io.qross.pql.${statement.caption}")
                            .getDeclaredMethod("execute", classOf[PQL], classOf[Statement])
                            .invoke(statement.instance, this, statement)

                    if (BREAK) break
                }
                else {
                    Class.forName(s"io.qross.pql.${statement.caption}")
                            .getDeclaredMethod("execute", classOf[PQL])
                            .invoke(statement.instance, this)

                    if (BREAK) break
                }
            }
        }
    }

    //程序变量相关
    //root块存储程序级全局变量
    //其他局部变量在子块中
    def updateVariable(field: String, value: DataCell): Unit = {
        val symbol = field.take(1)
        val name = field.drop(1).$trim("(", ")").toUpperCase()

        if (symbol == "$") {
            //局部变量
            var found = false
            breakable {
                for (i <- FOR$VARIABLES.indices) {
                    if (FOR$VARIABLES(i).contains(name)) {
                        FOR$VARIABLES(i).set(name, value)
                        found = true
                        break
                    }
                }
            }

            if (!found) {
                breakable {
                    for (i <- EXECUTING.indices) {
                        if (EXECUTING(i).containsVariable(name)) {
                            EXECUTING(i).setVariable(name, value)
                            found = true
                            break
                        }
                    }
                }
            }

            if (!found) {
                EXECUTING.head.setVariable(name, value)
            }
        }
        else if (symbol == "@") {
            GlobalVariable.set(name, value, credential.getInt("userid"))
        }
        else if (symbol == "%") {
            if (this.jobId > 0 && JDBC.hasQrossSystem) {
                val ds = DataSource.QROSS
                if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_jobs_variables'")) {
                    ds.executeNonQuery("INSERT INTO qross_jobs_variables (job_id, variable_name, variable_type, variable_value) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE variable_type=?, variable_value=?", jobId, name, value.dataType.typeName, value.value, value.dataType.typeName, value.value)
                }
                ds.close()
            }
        }
    }

    //变量名称存储时均为大写, 即在执行过程中不区分大小写
    def findVariable(field: String): DataCell = {

        val symbol = field.take(1)
        val name = field.drop(1).toUpperCase()

        var cell = DataCell.UNDEFINED

        if (symbol == "$") {
            var found = false
            breakable {
                for (i <- FOR$VARIABLES.indices) {
                    if (FOR$VARIABLES(i).contains(name)) {
                        cell = FOR$VARIABLES(i).get(name)
                        found = true
                        break
                    }
                }
            }

            if (!found) {
                breakable {
                    for (i <- EXECUTING.indices) {
                        if (EXECUTING(i).containsVariable(name)) {
                            cell = EXECUTING(i).getVariable(name)
                            found = true
                            break
                        }
                    }
                }
            }

            if (!found) {
                if (root.containsVariable(name)) {
                    cell = root.getVariable(name)
                }
            }
        }
        else if (symbol == "@") {
            cell = GlobalVariable.get(name, this)
        }
        else if (symbol == "%") {
            if (this.jobId > 0 && JDBC.hasQrossSystem) {
                val ds = DataSource.QROSS
                if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_jobs_variables'")) {
                    val row = ds.executeDataRow("SELECT variable_type, variable_value FROM qross_jobs_variables WHERE job_id=? AND variable_name=?", jobId, name)
                    if (row.nonEmpty) {
                        cell = DataCell(row.getString("variable_type") match {
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
                    }
                }
                ds.close()
            }
        }

        cell
    }

    //传递参数和数据, Spring Boot的httpRequest参数
    def place(name: String, value: String): PQL = {
        this.SQL = this.SQL.replaceArguments(Map[String, String](name -> value))
        ARGUMENTS.set(name, value, DataType.TEXT)
        root.setVariable(name, value, DataType.TEXT)
        this
    }

    def place(args: DataRow): PQL = {
        if (args != null && args.nonEmpty) {
            this.SQL = this.SQL.replaceArguments(args)
            ARGUMENTS.combine(args)
            root.variables.combine(args)
        }
        this
    }

    def place(args: (String, String)*): PQL = {
        this.SQL = this.SQL.replaceArguments(args: _*)

        args.foreach(pair => {
            ARGUMENTS.set(pair._1, pair._2, DataType.TEXT)
            root.setVariable(pair._1, pair._2, DataType.TEXT)
        })
        this
    }

    def place(map: java.util.Map[String, Object]): PQL = {
        this.SQL = this.SQL.replaceArguments(map)

        map.keySet().forEach(key => {
            ARGUMENTS.set(key, map.get(key))
            root.setVariable(key, map.get(key))
        })
        this
    }

    def place(args: Map[String, String]): PQL = {
        this.SQL = this.SQL.replaceArguments(args)

        args.foreach(pair => {
            ARGUMENTS.set(pair._1, pair._2, DataType.TEXT)
            root.setVariable(pair._1, pair._2, DataType.TEXT)
        })

        this
    }

    def place(queryOrJsonString: String): PQL = {
        if (queryOrJsonString != "") {
            if (queryOrJsonString.bracketsWith("{", "}")) {
                place(Json.fromText(queryOrJsonString).parseRow("/"))
            }
            else if (queryOrJsonString.contains("=")) {
                place(queryOrJsonString.splitToMap().map(pair => (pair._1.trim(), pair._2.decodeURL())))
            }
        }
        this
    }

    //设置默认值, 变量没有时才赋值
    def placeDefault(queryOrJsonString: String): PQL = {
        if (queryOrJsonString != "") {
            if (queryOrJsonString.bracketsWith("{", "}")) {
                val row = Json.fromText(queryOrJsonString).parseRow("/")
                this.SQL = this.SQL.replaceArguments(row)
                row.fields.foreach(field => {
                    if (!ARGUMENTS.contains(field)) {
                        ARGUMENTS.set(field, row.getCell(field))
                    }
                    if (!root.containsVariable(field)) {
                        root.setVariable(field, row.getCell(field))
                    }
                })
            }
            else {
                val map = queryOrJsonString.splitToMap().map(pair => (pair._1.trim(), pair._2.decodeURL()))
                this.SQL = this.SQL.replaceArguments(map)
                map.foreach(pair => {
                    if (!ARGUMENTS.contains(pair._1)) {
                        ARGUMENTS.set(pair._1, pair._2, DataType.TEXT)
                    }
                    if (!root.containsVariable(pair._1)) {
                        root.setVariable(pair._1, pair._2, DataType.TEXT)
                    }
                })
            }
        }
        this
    }

    //设置单个变量的值
    def set(variableName: String, value: Any): PQL = {
        root.setVariable(variableName, value)
        this
    }

    //设置多个变量的值
    def set(variables: (String, Any)*): PQL = {
        variables.foreach(variable => {
            root.setVariable(variable._1, variable._2)
        })
        this
    }

    //设置多个变量的值
    def set(row: DataRow): PQL = {
        root.variables.combine(row)
        this
    }

    def set(args: Map[String, String]): PQL = {
        args.foreach(pair => {
            root.setVariable(pair._1, pair._2, DataType.TEXT)
        })
        this
    }

    def set(queryOrJsonString: String): PQL = {
        if (queryOrJsonString != "") {
            if (queryOrJsonString.bracketsWith("{", "}")) {
                set(Json.fromText(queryOrJsonString).parseRow("/"))
            }
            else {
                queryOrJsonString.splitToMap().foreach(pair => {
                    root.setVariable(pair._1.trim(), pair._2.decodeURL(), DataType.TEXT)
                })
            }
        }
        this
    }

    def set(model: java.util.Map[String, Any]): PQL = {
        model.keySet().forEach(key => {
            if (key.startsWith("@")) {
                //全局变量
                GlobalVariable.SYSTEM.set(key.drop(1), model.get(key))
            }
            else if (key.startsWith("$")) {
                //用户变量
                root.setVariable(key.drop(1), model.get(key))
            }
            else if (key.startsWith("#")) {
                //用户登录信息
                credential.set(key.drop(1), model.get(key))
            }
            else {
                root.setVariable(key, model.get(key))
            }
        })
        this
    }

    def asCommandOf(jobId: Int): PQL = {
        this.jobId = jobId
        this
    }

    def signIn(userId: Int, userName: String, role: String): PQL = {
        credential.set("userid", userId)
        credential.set("username", userName)
        credential.set("role", role)

        Configurations.load(userId)

        this
    }

    def signIn(userId: Int, userName: String, role: String, info: (String, Any)*): PQL = {
        credential.set("userid", userId)
        credential.set("username", userName)
        credential.set("role", role)
        info.foreach(kv => credential.set(kv._1, kv._2))

        Configurations.load(userId)

        this
    }

    def signIn(info: (String, Any)*): PQL = {
        info.foreach(kv => credential.set(kv._1, kv._2))

        Configurations.load(credential.getInt("userid", 0))

        this
    }

    def signIn(info : java.util.Map[String, Any]): PQL = {
        for (kv <- info.asScala) {
            credential.set(kv._1, kv._2)
        }

        if (!credential.contains("userid")) {
            if (credential.contains("uid")) {
                credential.set("userid", credential.getCell("uid"))
            }
            else if (credential.contains("id")) {
                credential.set("userid", credential.getCell("id"))
            }
        }
        if (!credential.contains("username")) {
            if (credential.contains("name")) {
                credential.set("username", credential.getCell("name"))
            }
        }

        if (credential.contains("userid")) {
            Configurations.load(credential.getInt("userid"))
        }

        this
    }

    def signIn(userId: Int, userName: String, role: String, info : java.util.Map[String, Any]): PQL = {

        credential.set("userid", userId)
        credential.set("username", userName)
        credential.set("role", role)
        for (kv <- info.asScala) {
            credential.set(kv._1, kv._2)
        }

        Configurations.load(userId)

        this
    }

    def $stash(value: DataCell): String = {
        this.values += value
        s"~value[${this.values.size - 1}]"
    }

    //运行但不关闭 DataHub
    def $run(): PQL = {
        this.parseAll()
        recognizeParameters().forEach(name => {
            if (!root.containsVariable(name)) {
                root.setVariable(name, DataCell.UNDEFINED)
            }
        })
        EXECUTING.push(root)
        this.executeStatements(root.statements)
        dh.clear()

        this
    }

    def $return: Any = {
        if (RESULT.nonEmpty) {
            if (embedded) {
                //嵌入式输出全部
                RESULT.map {
                    case table: DataTable => table.toString
                    case row: DataRow => row.toString
                    case dt: DateTime => dt.toString
                    case str: String => str
                    case o: AnyRef => Json.serialize(o)
                    case null => null
                    case x => x.toString
                }.mkString
            }
            else if (RESULT.size == 1) {
                //一项
                RESULT.head match {
                    case table: DataTable => table.toJavaMapList
                    case row: DataRow => row.toJavaMap
                    case dt: DateTime => dt.toString //.useQuotes("\"")
                    case str: String => str //.useQuotes("\"")
                    case o => o
                }
            }
            else {
                //多基输出为数组形式
                RESULT.map {
                        case table: DataTable => table.toJavaMapList
                        case row: DataRow => row.toJavaMap
                        case dt: DateTime => dt.toString //.useQuotes("\"")
                        case str: String => str //.useQuotes("\"")
                        case o => o
                }.asJava
            }
        }
        else if (WORKING.nonEmpty) {
            //显式输出为空时才使用隐式输出
            WORKING.last match {
                case table: DataTable => table.toJavaMapList
                case row: DataRow => row.toJavaMap
                case dt: DateTime => dt.toString //.useQuotes("\"")
                case str: String => str //.useQuotes("\"")
                case o => o
            }
        }
        else if (AFFECTED_ROWS_OF_LAST_NON_QUERY > -1) {
            //都为空时输出最后一条非查询语句影响的行数
            AFFECTED_ROWS_OF_LAST_NON_QUERY
        }
        else {
            null
        }
    }

    //仅检查语句的正确性，返回第一个错误
    def check(): String = {
        try {
            this.parseAll()
            ""
        }
        catch {
            case e: Exception => e.getReferMessage
        }
    }

    private def recognizeExcludingParameters(statements: mutable.ArrayBuffer[Statement]): mutable.HashSet[String] = {
        val excluding = new mutable.HashSet[String]
        for (statement <- statements) {
            statement.caption match {
                case "SET" =>
                    excluding ++= statement.instance.asInstanceOf[SET].variables.map(_.drop(1))
                case "VAR" =>
                    excluding ++= statement.instance.asInstanceOf[VAR].variables.map(_._1.drop(1))
                case "FOR" =>
                    excluding ++= statement.instance.asInstanceOf[FOR].variables.map(_.drop(1))
                case "CALL" =>
                    excluding ++= """\$(\w+)\s*(?=:=)""".r.findAllMatchIn(statement.instance.asInstanceOf[CALL].sentence).map(_.group(1).toLowerCase())
                case _ =>
            }

            if (statement.statements.nonEmpty) {
                excluding ++= recognizeExcludingParameters(statement.statements)
            }
        }

        excluding
    }

    def recognizeParameters(): util.Set[String] = {
        if (root.statements.isEmpty) {
            this.parseAll()
        }

        val parameters = new util.TreeSet[String]()
        val excluding = new mutable.HashSet[String]

        (Solver.ARGUMENTS ++ Solver.USER_VARIABLE)
            .flatMap(_.findAllMatchIn(this.originalSQL.replaceAll("--.+?\\r?\\n", "").replaceAll("(?s)/\\*.+?\\*/", "")))
            .foreach(m => parameters.add(m.group(1).removeQuotes().toLowerCase()))

        this.USER$FUNCTIONS.values.foreach(f => {
            excluding ++= f.variables.fields
        })

        excluding ++= recognizeExcludingParameters(root.statements)

        excluding --= """(?i)\$(\w+)\s+IS\s+UNDEFINED""".r.findAllMatchIn(SQL).map(_.group(1).toLowerCase())

        parameters.removeAll(excluding.asJava)

        parameters
    }


    //运行并返回最多一个结果, 一般用在全局函数体中
    def call(): DataCell = {
        this.$run()
        this.dh.close()

        if (RESULT.nonEmpty) {
            DataCell(RESULT.last)
        }
        else if (WORKING.nonEmpty) {
            //显式输出为空时才使用隐式输出
            DataCell(WORKING.last)
        }
        else if (AFFECTED_ROWS_OF_LAST_NON_QUERY > -1) {
            //都为空时输出最后一条非查询语句影响的行数
            DataCell(AFFECTED_ROWS_OF_LAST_NON_QUERY, DataType.INTEGER)
        }
        else {
            DataCell.NULL
        }
    }

    //运行但关闭DataHub
    def run(): Any = {
        this.$run()
        this.dh.close()
        this.$return
    }

    //运行并输出，不关闭 DataHub
    def output: Any = {
        this.$run().$return
    }

    def close(): Unit = {
        this.dh.close()
    }
}