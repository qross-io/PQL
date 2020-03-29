package io.qross.pql

import io.qross.core.Authentication._
import io.qross.core._
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.fs.SourceFile
import io.qross.net.Json
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
import io.qross.time.DateTime
import javax.servlet.http.HttpServletRequest

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

        def setArgs(queries: java.util.Map[String, Array[String]]): DataHub = {
            PQL.placeParameters(queries)
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

    private[pql] var SQL: String = originalSQL

    val embedded: Boolean = SQL.startsWith(EMBEDDED) || SQL.bracketsWith("<", ">") || (SQL.contains("<%") && SQL.contains("%>"))
    if (embedded && SQL.startsWith(EMBEDDED)) {
        SQL = SQL.drop(9)
    }

    private[pql] val root: Statement = new Statement("ROOT", SQL)

    //结果集
    private[pql] val RESULT: ArrayBuffer[Any] = new ArrayBuffer[Any]()
    private[pql] var COUNT_OF_LAST_SELECT: Int = -1 //最后一个SELECT返回的结果数量
    private[pql] var AFFECTED_ROWS_OF_LAST_NON_QUERY: Int = -1  //最后一个非SELECT语句影响的数据表行数
    private[pql] var BOOL: Boolean = false //最后一个可返回Boolean类型的语句的执行结果

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

    private[pql] lazy val USER$FUNCTIONS = new mutable.LinkedHashMap[String, UserFunction]()

    private[pql] var breakCurrentLoop = false

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
            throw new SQLParseException("Control statement hasn't closed: " + PARSING.head.sentence)
        }
    }

    //解析入口
    def parseStatement(sentence: String): Unit = {
        val caption: String = {
            if ("(?i)^[a-z]+#".r.test(sentence)) {
                sentence.takeBefore("#")
            }
            else if ($BLANK.test(sentence)) {
                sentence.takeBefore($BLANK).toUpperCase
            }
            else {
                sentence.toUpperCase
            }
        }
        if (NON_QUERY_CAPTIONS.contains(caption)) {
            PARSING.head.addStatement(new Statement(caption, sentence))
        }
        else {
            try {
                Class.forName(s"io.qross.pql.$caption")
                        .getDeclaredMethod("parse", "".getClass, Class.forName("io.qross.pql.PQL"))
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
                    val SQL = statement.sentence.$restore(this)
                    AFFECTED_ROWS_OF_LAST_NON_QUERY = dh.set(SQL).AFFECTED_ROWS_OF_LAST_SET

                    if (dh.debugging) {
                        Output.writeLine("                                                                        ")
                        Output.writeLine(SQL)
                        Output.writeLine("------------------------------------------------------------------------")
                        Output.writeLine(s"$AFFECTED_ROWS_OF_LAST_NON_QUERY row(s) affected. ")
                    }
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
                else if (STATEMENTS.contains(statement.caption)) {
                    Class.forName(s"io.qross.pql.${statement.caption}")
                            .getDeclaredMethod("execute",
                                Class.forName(s"io.qross.pql.PQL"),
                                Class.forName(s"io.qross.pql.Statement"))
                            .invoke(statement.instance, this, statement)
                }
                else {
                    Class.forName(s"io.qross.pql.${statement.caption}")
                            .getDeclaredMethod("execute",
                                Class.forName(s"io.qross.pql.PQL"))
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
        val name = field.takeAfter(0).$trim("(", ")").toUpperCase()

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
            //全局变量
            GlobalVariable.set(name, value, dh.userId, dh.roleName)
        }
    }

    //变量名称存储时均为大写, 即在执行过程中不区分大小写
    def findVariable(field: String): DataCell = {

        val symbol = field.take(1)
        val name = field.takeAfter(0).$trim("(", ")").toUpperCase()

        var cell = DataCell.NOT_FOUND

        if (symbol == "$") {
            breakable {
                for (i <- FOR$VARIABLES.indices) {
                    if (FOR$VARIABLES(i).contains(name)) {
                        cell = FOR$VARIABLES(i).get(name)
                        break
                    }
                }
            }

            if (EXECUTING.nonEmpty) {
                breakable {
                    for (i <- EXECUTING.indices) {
                        if (EXECUTING(i).containsVariable(name)) {
                            cell = EXECUTING(i).getVariable(name)
                            break
                        }
                    }
                }
            }
            else {
                if (root.containsVariable(name)) {
                    cell = root.getVariable(name)
                }
            }
        }
        else if (symbol == "@") {
            //全局变量
            cell = GlobalVariable.get(name, this)

            //未找到即忽略, 因为MySQL的局部变量也是以 @ 开头
//            if (cell.invalid) {
//                throw new SQLExecuteException(s"Global variable $field is not found.")
//            }
        }

        cell
    }

    //传递参数和数据, Spring Boot的httpRequest参数
    def place(name: String, value: String): PQL = {
        this.SQL = this.SQL.replaceArguments(Map[String, String](name -> value))
        this
    }

    def place(args: (String, String)*): PQL = {
        this.SQL = this.SQL.replaceArguments(args.toMap[String, String])
        this
    }

    def placeParameters(queries: java.util.Map[String, Array[String]]): PQL = {
        this.SQL = this.SQL.replaceArguments(
                queries.asScala
                        .map(kv => (kv._1, kv._2.headOption.getOrElse(""))).toMap[String, String])

        queries.keySet().forEach(key => {
            root.setVariable(key, queries.get(key).headOption.getOrElse(""))
        })
        this
    }

    def place(map: java.util.Map[String, String]): PQL = {
        this.SQL = this.SQL.replaceArguments(map.asScala.toMap)

        map.keySet().forEach(key => {
            root.setVariable(key, map.get(key))
        })
        this
    }

    def place(args: Map[String, String]): PQL = {
        this.SQL = this.SQL.replaceArguments(args)
        this
    }

    def place(queryString: String): PQL = {
        if (queryString != "") {
            this.SQL = this.SQL.replaceArguments(queryString.$restore(this, "").$split())
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
            set(queryString.$split().toRow)
        }
        this
    }

    def set(model: java.util.Map[String, Any]): PQL = {
        model.keySet().forEach(key => {
            root.setVariable(key, model.get(key))
        })
        this
    }

    def setHttpRequest(request: HttpServletRequest): PQL = {

        root.setVariable("request", new DataRow(
            "method" -> request.getMethod,
            "queryString" -> request.getQueryString,
            "domain" -> request.getServerName,
            "protocol" -> request.getRequestURL.toString.takeBefore(":"),
            "path" -> request.getRequestURI,
            "host" -> request.getRequestURL.toString.takeBefore(request.getRequestURI)
        ))

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
                RESULT.head match {
                    case table: DataTable => table.toJavaMapList
                    case row: DataRow => row.toJavaMap
                    case dt: DateTime => dt.toString.useQuotes("\"")
                    case str: String => str.useQuotes("\"")
                    case o => o
                }
            }
            else {
                RESULT.map {
                        case table: DataTable => table.toJavaMapList
                        case row: DataRow => row.toJavaMap
                        case dt: DateTime => dt.toString.useQuotes("\"")
                        case str: String => str.useQuotes("\"")
                        case o => o
                }.asJava
            }
        }
        else if (AFFECTED_ROWS_OF_LAST_NON_QUERY > -1) {
            AFFECTED_ROWS_OF_LAST_NON_QUERY
        }
        else {
            null
        }
    }

    def output: Any = {
        dh.close()
        this.$return
    }

    //运行但关闭DataHub
    def run(): Any = {
        this.$run()
        this.output
    }

    def close(): Unit = {
        this.dh.close()
    }
}