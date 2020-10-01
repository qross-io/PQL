package io.qross.pql

import io.qross.core._
import io.qross.exception.{ExtensionNotFoundException, SQLParseException}
import io.qross.ext.TypeExt._
import io.qross.fs.SourceFile
import io.qross.net.Json
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
import io.qross.setting.Language
import io.qross.time.DateTime

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object PQL {

    //打开但不运行
    def open(SQL: String): PQL = {
        new PQL(SQL, new DataHub())
    }

    def openEmbedded(SQL: String): PQL = {
        new PQL(EMBEDDED + SQL, new DataHub())
    }

    //直接运行
    def run(SQL: String): Any = {
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
        new PQL(EMBEDDED + SourceFile.read(path), new DataHub())
    }

    def openEmbeddedFile(path: String, dh: DataHub): PQL = {
        new PQL(EMBEDDED + SourceFile.read(path), dh)
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
            dh.plug("PQL", new PQL(EMBEDDED + SQL, dh))
        }

        def openFilePQL(filePath: String): DataHub = {
            dh.plug("PQL", new PQL(SourceFile.read(filePath), dh))
        }

        def openEmbeddedFilePQL(filePath: String): DataHub = {
            dh.plug("PQL", new PQL(EMBEDDED + SourceFile.read(filePath), dh))
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
            new PQL(if (!SQL.startsWith(EMBEDDED)) EMBEDDED + SQL else SQL, dh).$run().$return
        }

        def runFile(filePath: String): Any = {
            new PQL(SourceFile.read(filePath), dh).$run().$return
        }

        def runEmbeddedFile(filePath: String): Any = {
            new PQL(EMBEDDED + SourceFile.read(filePath), dh).$run().$return
        }
    }
}

class PQL(val originalSQL: String, val dh: DataHub) {

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

    private[pql] val embedded: Boolean = SQL.startsWith(EMBEDDED) || SQL.bracketsWith("<", ">") || (SQL.contains("<%") && SQL.contains("%>"))
    if (embedded && SQL.startsWith(EMBEDDED)) {
        SQL = SQL.drop(9)
    }
    private[pql] var language: String = Language.name
    private[pql] var languageModules: String = ""

    private[pql] val root: Statement = new Statement("ROOT", SQL)

    //结果集
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
                //内嵌语言模块
                Language.include.findAllMatchIn(SQL)
                        .foreach(m => {
                            if (languageModules == "") {
                                languageModules = m.group(1)
                            }
                            else {
                                languageModules += "," + m.group(1)
                            }
                            SQL = SQL.replace(m.group(0), "")
                        })

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
                            List[String]("ECHO " + block)
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
                }
                else {
                    Class.forName(s"io.qross.pql.${statement.caption}")
                            .getDeclaredMethod("execute", classOf[PQL])
                            .invoke(statement.instance, this)
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
            GlobalVariable.set(name, value.value, credential.getInt("userid"), credential.getString("role"))
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
                            break
                        }
                    }
                }
            }
        }
        else if (symbol == "@") {
            cell = GlobalVariable.get(name, this)
        }

        cell
    }

    //变量名称存储时均为大写, 即在执行过程中不区分大小写
    def containsVariable(field: String): Boolean = {

        val symbol = field.take(1)
        val name = field.drop(1).toUpperCase()

        var found = false

        if (symbol == "$") {
            breakable {
                for (i <- FOR$VARIABLES.indices) {
                    if (FOR$VARIABLES(i).contains(name)) {
                        found = true
                        break
                    }
                }
            }

            if (!found) {
                breakable {
                    for (i <- EXECUTING.indices) {
                        if (EXECUTING(i).containsVariable(name)) {
                            found = true
                            break
                        }
                    }
                }
            }
        }
        else if (symbol == "@") {
            GlobalVariable.contains(name, this)
        }

        found
    }

    //传递参数和数据, Spring Boot的httpRequest参数
    def place(name: String, value: String): PQL = {
        this.SQL = this.SQL.replaceArguments(Map[String, String](name -> value))

        root.setVariable(name, value)
        this
    }

    def place(args: (String, String)*): PQL = {
        this.SQL = this.SQL.replaceArguments(args.toMap[String, String])

        args.foreach(pair => {
            root.setVariable(pair._1, pair._2)
        })
        this
    }

    def place(map: java.util.Map[String, Object]): PQL = {
        this.SQL = this.SQL.replaceArguments(map.asScala.map(kv => (kv._1, kv._2.toString)).toMap)

        map.keySet().forEach(key => {
            root.setVariable(key, map.get(key))
        })
        this
    }

    def place(args: Map[String, String]): PQL = {
        this.SQL = this.SQL.replaceArguments(args)

        args.foreach(pair => {
            root.setVariable(pair._1, pair._2)
        })

        this
    }

    def place(queryString: String): PQL = {
        if (queryString != "") {
            val map = queryString.$split()

            this.SQL = this.SQL.replaceArguments(map)
            map.foreach(pair => {
                root.setVariable(pair._1, pair._2)
            })
        }
        this
    }

    //设置默认值, 变量没有时才赋值
    def placeDefault(queryString: String): PQL = {
        if (queryString != "") {
            val map = queryString.$split()

            this.SQL = this.SQL.replaceArguments(map)
            map.foreach(pair => {
                if (!root.containsVariable(pair._1)) {
                    root.setVariable(pair._1, pair._2)
                }
            })
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

    def set(queryString: String): PQL = {
        if (queryString != "") {
            queryString.$split().foreach(pair => {
                root.setVariable(pair._1, pair._2)
            })
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

    def signIn(userId: Int, userName: String, role: String): PQL = {
        credential.set("userid", userId)
        credential.set("username", userName)
        credential.set("role", role)

        GlobalVariable.loadUserVariables(userId)

        this
    }

    def signIn(userId: Int, userName: String, role: String, info: (String, Any)*): PQL = {
        credential.set("userid", userId)
        credential.set("username", userName)
        credential.set("role", role)
        info.foreach(kv => credential.set(kv._1, kv._2))

        GlobalVariable.loadUserVariables(userId)

        this
    }

    def signIn(info: (String, Any)*): PQL = {
        info.foreach(kv => credential.set(kv._1, kv._2))

        val userId = credential.getInt("userid", 0)
        if (userId > 0) {
            GlobalVariable.loadUserVariables(userId)
        }

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
            GlobalVariable.loadUserVariables(credential.getInt("userid"))
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

        GlobalVariable.loadUserVariables(userId)

        this
    }

    def setLanguage(languageName: String): PQL = {
        this.language = Language.verify(languageName)
        this
    }

    def $stash(value: DataCell): String = {
        this.values += value
        s"~value[${this.values.size - 1}]"
    }

    //运行但不关闭DataHub
    def $run(): PQL = {
        this.parseAll()
        EXECUTING.push(root)
        this.executeStatements(root.statements)
        dh.clear()

        this
    }

    def $return: Any = {
        if (RESULT.nonEmpty) {
            if (embedded) {
                //嵌入式输出全部
                RESULT.map{
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
                    case dt: DateTime => dt.toString.useQuotes("\"")
                    case str: String => str.useQuotes("\"")
                    case o => o
                }
            }
            else {
                //多基输出为数组形式
                RESULT.map {
                        case table: DataTable => table.toJavaMapList
                        case row: DataRow => row.toJavaMap
                        case dt: DateTime => dt.toString.useQuotes("\"")
                        case str: String => str.useQuotes("\"")
                        case o => o
                }.asJava
            }
        }
        else if (WORKING.nonEmpty) {
            //显式输出为空时才使用隐式输出
            WORKING.last match {
                case table: DataTable => table.toJavaMapList
                case row: DataRow => row.toJavaMap
                case dt: DateTime => dt.toString.useQuotes("\"")
                case str: String => str.useQuotes("\"")
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

    //运行但关闭DataHub
    def run(): Any = {
        this.$run()
        this.dh.close()
        this.$return
    }

    //运行并输出，不关闭DataHub
    def output: Any = {
        this.$run().$return
    }

    def close(): Unit = {
        this.dh.close()
    }
}