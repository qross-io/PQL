package io.qross.sql

import java.io.{File, FileNotFoundException}
import java.util.regex.Matcher

import io.qross.core.Authentication._
import io.qross.core._
import io.qross.ext.TypeExt._
import io.qross.ext.{ArgumentMap, Output, ParameterMap}
import io.qross.fs.FilePath._
import io.qross.fs.ResourceFile
import io.qross.net.Json._
import io.qross.net.{Http, Json}
import io.qross.setting.Properties
import io.qross.sql.Patterns._
import io.qross.sql.Solver._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.control.Breaks._

object PSQL {

    //打开文件但不运行
    def openFile(path: String): PSQL = {

        var SQL = ""
        val resource = ResourceFile.open(path)
        if (resource.exists) {
            SQL = resource.output
        }
        else {
            val file = new File(path.locate())
            if (file.exists()) {
                SQL = Source.fromFile(file, "UTF-8").mkString
            }
            else {
                throw new FileNotFoundException(s"File $path doesn't exists.")
            }
        }

        new PSQL(SQL, new DataHub())
    }

    //直接运行
    def runFile(path: String): Any = {
        PSQL.openFile(path).run()
    }

    //打开但不运行
    def open(SQL: String): PSQL = {
        new PSQL(SQL, new DataHub())
    }

    //直接运行
    def run(SQL: String): Any = {
        PSQL.open(SQL).run()
    }

    implicit class DataHub$PSQL(val dh: DataHub) {

        def PSQL: PSQL = {
            if (dh.slots("PSQL")) {
                dh.pick("PSQL").asInstanceOf[PSQL]
            }
            else {
                throw new ExtensionNotFoundException("Must use openSQL/openFileSQL/openResourceSQL method to open a PSQL first.")
            }
        }

        def openSQL(SQL: String): DataHub = {
            dh.plug("PSQL", new PSQL(SQL, dh))
        }

        def openFileSQL(filePath: String): DataHub = {
            dh.plug("PSQL", new PSQL(Source.fromFile(filePath.locate()).mkString, dh))
        }

        def openResourceSQL(resourcePath: String): DataHub = {
            dh.plug("PSQL", new PSQL(ResourceFile.open(resourcePath).output, dh))
            dh
        }

        def setArgs(args: Any): DataHub = {
            PSQL.assign(args)
            dh
        }

        def setVariable(name: String, value: Any): DataHub = {
            PSQL.set(name, value)
            dh
        }

        def run(): Any = {
            PSQL.$run().$return
        }

        def run(SQL: String): Any = {
            new PSQL(SQL, dh).$run().$return
        }

        def runFileSQL(filePath: String, outputType: String = OUTPUT.TABLE): Any = {
            new PSQL(Source.fromFile(filePath.locate()).mkString, dh).$run().$return
        }

        def runResourceSQL(resourcePath: String, outputType: String = OUTPUT.TABLE): Any = {
            new PSQL(ResourceFile.open(resourcePath).output, dh).$run().$return
        }
    }
}

class PSQL(val originalSQL: String, val dh: DataHub) {

    //字符串 ~char[n]
    val chars: ArrayBuffer[String] = new ArrayBuffer[String]()
    //富字符串 ~string[n]
    val strings: ArrayBuffer[String] = new ArrayBuffer[String]()
    //计算过程中的中间结果~value[n]
    val values: ArrayBuffer[DataCell] = new ArrayBuffer[DataCell]()

    private var SQL: String = originalSQL

    private val root: Statement = new Statement("ROOT", SQL)
    private var m: Matcher = _

    //结果集
    private var RESULT: Any = _
    var ROWS: Int = -1 //最后一个SELECT返回的结果数量
    var AFFECTED: Int = -1  //最后一个非SELECT语句影响的数据表行数

    //正在解析的所有语句, 控制语句包含ELSE和ELSE_IF
    private val PARSING = new mutable.ArrayStack[Statement]
    //正在执行的控制语句
    private val EXECUTING = new mutable.ArrayStack[Statement]
    //待关闭的控制语句，如IF, FOR, WHILE等，不保存ELSE和ELSE_IF
    private val TO_BE_CLOSE = new mutable.ArrayStack[Statement]
    //IF条件执行结果
    private val IF_BRANCHES = new mutable.ArrayStack[Boolean]
    //FOR语句循环项变量值
    private val FOR_VARIABLES = new mutable.ArrayStack[ForVariables]

    private var breakCurrentLoop = false

    //解析器
    private val PARSER = Map[String, String => Unit](
        "IF" ->  parseIF,
        "ELSE" -> parseELSE,
        "ELSIF" -> parseELSE,
        "END" -> parseEND,
        "FOR" -> parseFOR,
        "WHILE" -> parseWHILE,
        "EXIT" -> parseEXIT,
        "CONTINUE" -> parseCONTINUE,
        "SET" -> parseSET,
        "OPEN" -> parseOPEN,
        "USE" -> parseUSE,
        "SAVE" -> parseSAVE,
        "CACHE" -> parseCACHE,
        "TEMP" -> parseTEMP,
        "GET" -> parseGET,
        "PASS" -> parsePASS,
        "PUT" -> parsePUT,
        "PREP" -> parsePREP,
        "OUTPUT" -> parseOUTPUT,
        "ECHO" -> parseECHO,
        "PRINT" -> parsePRINT,
        "SHOW" -> parseSHOW,
        "RUN" -> parseRUN,
        "SELECT" -> parseSELECT,
        "REQUEST" -> parseREQUEST,
        "PARSE" -> parsePARSE,
        "DEBUG" -> parseDEBUG
    )

    //执行器
    private val EXECUTOR = Map[String, Statement => Unit](
        "IF" -> executeIF,
        "ELSE_IF" -> executeELSE_IF,
        "ELSE" -> executeELSE,
        "END_IF" -> executeEND_IF,
        "FOR" -> executeFOR,
        "WHILE" -> executeWHILE,
        "END_LOOP" -> executeEND_LOOP,
        "SET" -> executeSET,
        "USE" -> executeUSE,
        "OPEN" -> executeOPEN,
        "SAVE" -> executeSAVE, //
        "CACHE" -> executeCACHE,
        "TEMP" -> executeTEMP,
        "GET" -> executeGET,
        "PASS" -> executePASS,
        "PUT" -> executePUT,
        "PREP" -> executePREP,
        "OUTPUT" -> executeOUTPUT,
        "ECHO" -> executeECHO,
        "PRINT" -> executePRINT,
        "SHOW" -> executeSHOW,
        "RUN" -> executeRUN,
        "SELECT" -> executeSELECT,
        "REQUEST" -> executeREQUEST,
        "PARSE" -> executePARSE,
        "DEBUG" -> executeDEBUG
    )

    //开始解析
    private def parseAll(): Unit = {

        //check arguments
        ARGUMENT.findAllMatchIn(SQL).foreach(m => Output.writeWarning(s"Argument ${m.group(0)} is not assigned."))

        SQL = SQL.cleanCommentsAndStashChars(this)

        //开始解析
        PARSING.push(root)

        val sentences = SQL.split(";").map(str => str.trim)
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
        val caption: String = if ($BLANK.test(sentence)) { sentence.takeBefore($BLANK).toUpperCase } else sentence.toUpperCase
        if (PARSER.contains(caption)) {
            PARSER(caption)(sentence)
        }
        else if (NON_QUERY_CAPTIONS.contains(caption)) {
            PARSING.head.addStatement(new Statement(caption, sentence))
        }
        else {
            throw new SQLParseException("Unrecognized or unsupported sentence: " + sentence)
        }
    }

    private def parseIF(sentence: String): Unit = {
        if ({m = $IF.matcher(sentence); m}.find) {
            val $if: Statement = new Statement("IF", m.group(0), new ConditionGroup(m.group(1)))
            PARSING.head.addStatement($if)
            //只进栈
            PARSING.push($if)
            //待关闭的控制语句
            TO_BE_CLOSE.push($if)
            //继续解析第一条子语句
            parseStatement(sentence.takeAfter(m.group(0)).trim())
        }
        else {
            throw new SQLParseException("Incorrect IF sentence: " + sentence)
        }
    }

    private def parseELSE(sentence: String): Unit = {
        if ({m = $ELSE_IF.matcher(sentence); m}.find) {
            val $elsif: Statement = new Statement("ELSE_IF", m.group(0), new ConditionGroup(m.group(1)))
            if (PARSING.isEmpty || (!(PARSING.head.caption == "IF") && !(PARSING.head.caption == "ELSE_IF"))) {
                throw new SQLParseException("Can't find previous IF or ELSE IF clause: " + m.group(0))
            }
            //先出栈再进栈
            PARSING.pop()
            PARSING.head.addStatement($elsif)
            PARSING.push($elsif)
            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else if ({m = $ELSE.matcher(sentence); m}.find) {
            val $else: Statement = new Statement("ELSE")
            if (PARSING.isEmpty || (!(PARSING.head.caption == "IF") && !(PARSING.head.caption == "ELSE_IF"))) {
                throw new SQLParseException("Can't find previous IF or ELSE IF clause: " + m.group(0))
            }
            //先出栈再进栈
            PARSING.pop()
            PARSING.head.addStatement($else)
            PARSING.push($else)
            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else {
            throw new SQLParseException("Incorrect ELSE or ELSIF sentence: " + sentence)
        }
    }

    private def parseEND(sentence: String): Unit = {
        if ({m = $END_IF.matcher(sentence); m}.find) {
            //检查IF语句是否正常闭合
            if (TO_BE_CLOSE.isEmpty) {
                throw new SQLParseException("Can't find IF clause: " + m.group)
            }
            else if (!(TO_BE_CLOSE.head.caption == "IF")) {
                throw new SQLParseException(TO_BE_CLOSE.head.caption + " hasn't closed: " + TO_BE_CLOSE.head.sentence)
            }
            else {
                TO_BE_CLOSE.pop()
            }
            val $endIf: Statement = new Statement("END_IF")
            //只出栈
            PARSING.pop()
            PARSING.head.addStatement($endIf)
        }
        else if ({m = $END_LOOP.matcher(sentence); m}.find) {
            //检查FOR语句是否正常闭合
            if (TO_BE_CLOSE.isEmpty) {
                throw new SQLParseException("Can't find FOR or WHILE clause: " + m.group)
            }
            else if (!Set("FOR" , "WHILE").contains(TO_BE_CLOSE.head.caption)) {
                throw new SQLParseException(TO_BE_CLOSE.head.caption + " hasn't closed: " + TO_BE_CLOSE.head.sentence)
            }
            else {
                TO_BE_CLOSE.pop()
            }
            val $endLoop: Statement = new Statement("END_LOOP")
            //只出栈
            PARSING.pop()
            PARSING.head.addStatement($endLoop)
        }
        else {
            throw new SQLParseException("Incorrect END sentence: " + sentence)
        }
    }

    private def parseFOR(sentence: String): Unit = {

        if ({m = $FOR.matcher(sentence); m}.find) {
            val $for: Statement = new Statement("FOR", m.group(0), new FOR(m.group(1).trim(), m.group(2).trim()))

            PARSING.head.addStatement($for)
            //只进栈
            PARSING.push($for)
            //待关闭的控制语句
            TO_BE_CLOSE.push($for)
            //继续解析子语句
            parseStatement(sentence.takeAfter(m.group(0)).trim())
        }
        else {
            throw new SQLParseException("Incorrect FOR sentence: " + sentence)
        }
    }

    private def parseWHILE(sentence: String): Unit = {
        if ({m = $WHILE.matcher(sentence); m}.find) {
            val $while: Statement = new Statement("WHILE", m.group(0), new ConditionGroup(m.group(1).trim))
            PARSING.head.addStatement($while)
            //只进栈
            PARSING.push($while)
            //待关闭的控制语句
            TO_BE_CLOSE.push($while)
            //继续解析子语句
            parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else {
            throw new SQLParseException("Incorrect WHILE sentence: " + sentence)
        }
    }

    private def parseEXIT(sentence: String): Unit = {
        var contained = false
        breakable {
            for (group <- PARSING) {
                if (group.caption == "FOR" || group.caption == "WHILE") {
                    contained = true
                    break
                }
            }
        }

        if ({m = $EXIT.matcher(sentence); m}.find) {
            if (contained) {
                val $exit: Statement = new Statement("EXIT", m.group(0), if (m.group(1) != null) new ConditionGroup(m.group(2).trim()) else null)
                PARSING.head.addStatement($exit)
            }
            else {
                throw new SQLParseException("EXIT must be contained in FOR or WHILE statement: " + sentence)
            }
        }
        else {
            throw new SQLParseException("Incorrect EXIT sentence: " + sentence)
        }
    }

    private def parseCONTINUE(sentence: String): Unit = {
        var contained = false
        breakable {
            for (group <- PARSING) {
                if (group.caption == "FOR" || group.caption == "WHILE") {
                    contained = true
                    break
                }
            }
        }

        if ({m = $CONTINUE.matcher(sentence); m}.find) {
            if (contained) {
                val $continue: Statement = new Statement("CONTINUE", m.group(0), if (m.group(1) != null) new ConditionGroup(m.group(2).trim()) else null)
                PARSING.head.addStatement($continue)
            }
            else {
                throw new SQLParseException("CONTINUE must be contained in FOR or WHILE statement: " + sentence)
            }
        }
        else {
            throw new SQLParseException("Incorrect CONTINUE sentence: " + sentence)
        }
    }

    private def parseSET(sentence: String): Unit = {
        if ({m = $SET.matcher(sentence); m}.find) {
            val $set: Statement = new Statement("SET", sentence, new SET(m.group(1).trim, m.group(2).trim))
            PARSING.head.addStatement($set)
        }
        else {
            throw new SQLParseException("Incorrect SET sentence: " + sentence)
        }
    }

    private def parseOPEN(sentence: String): Unit = {
        if ({m = $OPEN.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("OPEN", sentence, new OPEN(m.group(1).trim.split($BLANKS): _*)))
            if (m.group(2).trim == ":") {
                parseStatement(sentence.takeAfter(m.group(2)).trim)
            }
        }
        else {
            throw new SQLParseException("Incorrect OPEN sentence: " + sentence)
        }
    }

    private def parseUSE(sentence: String): Unit = {
        if ({m = $USE.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("USE", sentence, new USE(sentence.takeAfter($BLANK).trim)))
        }
        else {
            throw new SQLParseException("Incorrect USE sentence: " + sentence)
        }
    }

    private def parseSAVE(sentence: String): Unit = {
        //save as
        if ({m = $SAVE$AS.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("SAVE", sentence, new SAVE$AS(m.group(1).trim.split($BLANKS): _*)))
            if (m.group(2).trim == ":") {
                parseStatement(sentence.takeAfter(":").trim)
            }
        }
        else {
            throw new SQLParseException("Incorrect SAVE sentence: " + sentence)
        }
    }

    private def parseCACHE(sentence: String): Unit = {
        if ({m = $CACHE.matcher(sentence); m}.find) {
            val $cache = new Statement("CACHE", sentence.takeBefore("#"), new CACHE(m.group(1).trim, sentence.takeAfter("#").trim))
            PARSING.head.addStatement($cache)
        }
        else {
            throw new SQLParseException("Incorrect CACHE sentence: " + sentence)
        }
    }

    private def parseTEMP(sentence: String): Unit = {
        if ({m = $TEMP.matcher(sentence); m}.find) {
            val $temp = new Statement("TEMP", sentence.takeBefore("#"), new TEMP(m.group(1).trim, sentence.takeAfter("#").trim))
            PARSING.head.addStatement($temp)
        }
        else {
            throw new SQLParseException("Incorrect TEMP sentence: " + sentence)
        }
    }

    private def parseGET(sentence: String): Unit = {
        if ({m = $GET.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("GET", sentence, new GET(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect GET sentence: " + sentence)
        }
    }

    private def parsePASS(sentence: String): Unit = {
        if ({m = $PASS.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("PASS", sentence, new PASS(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect PASS sentence: " + sentence)
        }
    }

    private def parsePUT(sentence: String): Unit = {
        if ({m = $PUT.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("PUT", sentence, new PUT(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect PUT sentence: " + sentence)
        }
    }

    private def parsePREP(sentence: String): Unit = {
        if ({m = $PREP.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("PREP", sentence, new PREP(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect PREP sentence: " + sentence)
        }
    }

    private def parseOUTPUT(sentence: String): Unit = {
        if ({m = $OUTPUT.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("OUTPUT", sentence, new OUTPUT(m.group(1), sentence.takeAfter("#").trim)))
        }
        else {
            throw new SQLParseException("Incorrect OUTPUT sentence: " + sentence)
        }
    }

    private def parseECHO(sentence: String): Unit = {

    }

    private def parsePRINT(sentence: String): Unit = {
        if ({m = $PRINT.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("PRINT", sentence, new PRINT(m.group(1), m.group(2).trim)))
        }
        else {
            throw new SQLParseException("Incorrect PRINT sentence: " + sentence)
        }
    }

    private def parseSHOW(sentence: String): Unit = {
        if ({m = $SHOW.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("SHOW", sentence, new SHOW(m.group(1))))
        }
        else {
            throw new SQLParseException("Incorrect SHOW sentence: " + sentence)
        }
    }

    private def parseRUN(sentence: String): Unit = {
        if ({m = $RUN.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("RUN", sentence, new RUN(sentence.takeAfter(m.group(0)).trim())))
        }
        else {
            throw new SQLParseException("Incorrect RUN sentence: " + sentence)
        }
    }

    private def parseSELECT(sentence: String): Unit = {
        PARSING.head.addStatement(new Statement("SELECT", sentence))
    }

    private def parseREQUEST(sentence: String): Unit = {
        if ({m = $REQUEST.matcher(sentence); m}.find) {
            PARSING.head.addStatement(new Statement("REQUEST", sentence, new REQUEST(sentence.takeAfter(m.group(0)).trim())))
        }
        else {
            throw new SQLParseException("Incorrect REQUEST sentence: " + sentence)
        }
    }

    private def parsePARSE(sentence: String): Unit = {
        if ($PARSE.test(sentence)) {
            PARSING.head.addStatement(new Statement("PARSE", sentence, new PARSE(sentence.takeAfter($PARSE).trim())))
        }
        else {
            throw new SQLParseException("Incorrect PARSE sentence: " + sentence)
        }
    }

    private def parseDEBUG(sentence: String): Unit = {
        if ($DEBUG.test(sentence)) {
            PARSING.head.addStatement(new Statement("DEBUG", sentence, new DEBUG(sentence.takeAfter($DEBUG).trim())))
        }
        else {
            throw new SQLParseException("Incorrect DEBUG sentence: " + sentence)
        }
    }

    /* EXECUTE */

    private def executeIF(statement: Statement): Unit = {
        if (statement.instance.asInstanceOf[ConditionGroup].evalAll(this)) {
            IF_BRANCHES.push(true)
            EXECUTING.push(statement)
            this.execute(statement.statements)
        }
        else {
            IF_BRANCHES.push(false)
        }
    }

    private def executeELSE_IF(statement: Statement): Unit = {
        if (!IF_BRANCHES.last) {
            if (statement.instance.asInstanceOf[ConditionGroup].evalAll(this)) { //替换
                IF_BRANCHES.pop()
                IF_BRANCHES.push(true)
                EXECUTING.push(statement)

                this.execute(statement.statements)
            }
        }
    }

    private def executeELSE(statement: Statement): Unit = {
        if (!IF_BRANCHES.last) {
            IF_BRANCHES.pop()
            IF_BRANCHES.push(true)
            EXECUTING.push(statement)

            this.execute(statement.statements)
        }
    }

    private def executeEND_IF(statement: Statement): Unit = {
        //结束本次IF语句
        if (IF_BRANCHES.last) { //在IF成功时才会有语句块进行栈
            EXECUTING.pop()
        }
        IF_BRANCHES.pop()
    }

    private def executeFOR(statement: Statement): Unit = {
        val $for = statement.instance.asInstanceOf[FOR]
        val vars: ForVariables = $for.computeVariables(this)

        FOR_VARIABLES.push(vars)
        EXECUTING.push(statement)
        //根据loopMap遍历/
        breakable {
            while (vars.hasNext) {
                if (!breakCurrentLoop) {
                    this.execute(statement.statements)
                }
                else {
                    break
                }
            }
        }
    }

    //continue属于exit的子集
    private def executeCONTINUE(statement: Statement): Boolean = {

        var contained = false
        breakable {
            for (group <- EXECUTING) {
                if (group.caption == "FOR" || group.caption == "WHILE") {
                    contained = true
                    break
                }
            }
        }

        if (contained) {
            if (statement.instance == null) {
                true
            }
            else {
                statement.instance.asInstanceOf[ConditionGroup].evalAll(this)
            }
        }
        else {
            throw new SQLExecuteException("CONTINUE must be contained in FOR or WHILE statement: " + statement.sentence)
        }
    }

    private def executeEXIT(statement: Statement): Boolean = {

        var contained = false
        breakable {
            for (group <- EXECUTING) {
                if (group.caption == "FOR" || group.caption == "WHILE") {
                    contained = true
                    break
                }
            }
        }

        if (contained) {
            breakCurrentLoop = executeCONTINUE(statement)
            breakCurrentLoop
        }
        else {
            throw new SQLExecuteException("EXIT must be contained in FOR or WHILE statement: " + statement.sentence)
        }
    }

    private def executeWHILE(statement: Statement): Unit = {
        val whileCondition: ConditionGroup = statement.instance.asInstanceOf[ConditionGroup]
        EXECUTING.push(statement)
        breakable {
            while (whileCondition.evalAll(this)) {
                if (!breakCurrentLoop) {
                    this.execute(statement.statements)
                }
                else {
                    break
                }
            }
        }
    }

    private def executeEND_LOOP(statement: Statement): Unit = {
        //除了FOR以外还是WHILE循环
        if (EXECUTING.head.caption == "FOR") {
            FOR_VARIABLES.pop()
        }
        EXECUTING.pop()
        //重置break变量
        if (breakCurrentLoop) {
            breakCurrentLoop = false
        }
    }

    private def executeSET(statement: Statement): Unit = {
        statement.instance.asInstanceOf[SET].assign(this)
    }

    private def executeOPEN(statement: Statement): Unit = {
        val $open = statement.instance.asInstanceOf[OPEN]
        $open.sourceType match {
            case "CACHE" => dh.openCache()
            case "TEMP" => dh.openTemp()
            case "DEFAULT" => dh.openDefault()
            case "QROSS" => dh.openQross()
            case _ =>
                val connectionName = {
                    if ($RESERVED.test($open.connectionName)) {
                        if (!Properties.contains($open.connectionName)) {
                            throw new SQLExecuteException("Wrong connection name: " + $open.connectionName)
                        }
                        $open.connectionName
                    }
                    else {
                        $open.connectionName.$eval(this).asText
                    }
                }

                if ($open.databaseName == "") {
                    dh.open(connectionName)
                }
                else {
                    dh.open(connectionName, $open.databaseName.$eval(this).asText)
                }
        }
    }

    private def executeUSE(statement: Statement): Unit = {
        val $use = statement.instance.asInstanceOf[USE]
        dh.use($use.databaseName.$eval(this).asText)
    }

    private def executeSAVE(statement: Statement): Unit = {
        val $save = statement.instance.asInstanceOf[SAVE$AS]
        $save.targetType match {
            case "CACHE TABLE" => {
                dh.cache($save.targetName.$eval(this).asText)
            }
            case "TEMP TABLE" => {
                dh.temp($save.targetName.$eval(this).asText)
            }
            case "CACHE" => dh.saveAsCache()
            case "TEMP" => dh.saveAsTemp()
            case "JDBC" =>
                $save.targetName match {
                    case "DEFAULT" => dh.saveAsDefault()
                    case "QROSS" => dh.saveAsQross()
                    case _ =>
                        val connectionName =
                            if ($RESERVED.test($save.targetName)) {
                                if (!Properties.contains($save.targetName)) {
                                    throw new SQLExecuteException("Wrong connection name: " + $save.targetName)
                                }
                                $save.targetName
                            }
                            else {
                                $save.targetName.$eval(this).asText
                            }

                        if ($save.databaseName == "") {
                            dh.saveAs(connectionName)
                        }
                        else {
                            dh.saveAs(connectionName, $save.databaseName.$eval(this).asText)
                        }
                }
            case _ =>
        }
    }

    private def executeCACHE(statement: Statement): Unit = {
        val $cache = statement.instance.asInstanceOf[CACHE]
        dh.get($cache.selectSQL.$restore(this)).cache($cache.tableName.$eval(this).asText)
    }

    private def executeTEMP(statement: Statement): Unit = {
        val $temp = statement.instance.asInstanceOf[TEMP]
        dh.get($temp.selectSQL.$restore(this)).temp($temp.tableName.$eval(this).asText)
    }

    private def executeGET(statement: Statement): Unit = {
        val $get = statement.instance.asInstanceOf[GET]
        dh.buffer(new SELECT($get.selectSQL.$restore(this)).execute(this).asTable)
        //dh.get($get.selectSQL.$restore(this))
    }

    private def executePASS(statement: Statement): Unit = {
        val $pass = statement.instance.asInstanceOf[PASS]
        dh.pass($pass.selectSQL.$restore(this))
    }

    private def executePUT(statement: Statement): Unit = {
        val $put = statement.instance.asInstanceOf[PUT]
        dh.put($put.nonQuerySQL.$restore(this))
    }

    private def executePREP(statement: Statement): Unit = {
        val $prep = statement.instance.asInstanceOf[PREP]
        dh.prep($prep.nonQuerySQL.$restore(this))
    }

    private def executeOUTPUT(statement: Statement): Unit = {
        val $output = statement.instance.asInstanceOf[OUTPUT]

        $output.caption match {
            case "SELECT" =>
                val SQL = $output.content.$restore(this)
                $output.outputType match {
                    case OUTPUT.TABLE =>
                        val table = dh.executeDataTable(SQL)
                        RESULT = table
                        ROWS = table.size
                    case OUTPUT.ROW | OUTPUT.MAP | OUTPUT.OBJECT =>
                        val map = dh.executeDataRow(SQL)
                        RESULT = map
                        ROWS = if (map.isEmpty) 0 else 1
                    case OUTPUT.ARRAY | OUTPUT.LIST =>
                        val list = dh.executeSingleList(SQL)
                        RESULT = list
                        ROWS = list.size
                    case OUTPUT.VALUE =>
                        val value = dh.executeSingleValue(SQL)
                        RESULT = value
                        ROWS = if (value.isEmpty) 0 else 1
                    case _ =>
                }
            case "PARSE" =>
                val path = $output.content.takeAfter("""^PARSE\s""".r).trim.$eval(this).asText
                val table = dh.parseTable(path)
                $output.outputType match {
                    case OUTPUT.TABLE =>
                        RESULT = table
                        ROWS = table.size
                    case OUTPUT.ROW | OUTPUT.MAP | OUTPUT.OBJECT =>
                        RESULT = table.firstRow match {
                                    case Some(row) =>
                                        ROWS = 1
                                        row
                                    case None =>
                                        ROWS = 0
                                        null
                                }
                    case OUTPUT.ARRAY | OUTPUT.LIST =>
                        RESULT = table.toJavaList
                        ROWS = table.size
                    case OUTPUT.VALUE =>
                        RESULT =
                            table.firstRow match {
                                case Some(row) =>
                                    ROWS = 1
                                    row.columns.head._2
                                case None =>
                                    ROWS = 0
                                    null
                            }
                    case _ =>
                }
            case cap =>
                if (NON_QUERY_CAPTIONS.contains(cap)) {
                    //非查询语句仅返回受影响的行数, 输出类型即使设置也无效
                    AFFECTED = this.dh.executeNonQuery($output.content.$restore(this))
                }
                else {
                    //JSON, 输出类型设置无效
                    if ($output.content.bracketsWith("[", "]") || $output.content.bracketsWith("{", "}")) {
                        //对象或数组类型不能eval
                        RESULT = Json.fromText($output.content.$restore(this, "\""))
                                    .findNode("/")
                    }
                    else {
                        //仅VALUE类型
                        RESULT = new SHARP($output.content.$clean(this)).execute(this).value
                    }
                }
        }
    }

    private def executeECHO(statement: Statement): Unit = {
        
    }

    private def executePRINT(statement: Statement): Unit = {
        val $print = statement.instance.asInstanceOf[PRINT]
        val message = {
            if ($print.message.bracketsWith("(", ")")) {
                $print.message
                    .$trim("(", ")")
                    .split(",")
                    .map(m => {
                        m.$eval(this).mkString("\"")
                    })
                    .mkString(", ")
                    .bracket("(", ")")
            }
            else if ($print.message.bracketsWith("[", "]") || $print.message.bracketsWith("{", "}")) {
                $print.message.$restore(this, "\"")
            }
            else {
                $print.message.$eval(this).asText
            }
        }
        $print.messageType match {
            case "WARN" => Output.writeWarning(message)
            case "ERROR" => Output.writeException(message)
            case "DEBUG" => Output.writeDebugging(message)
            case "INFO" => Output.writeMessage(message)
            case "NONE" => Output.writeLine(message)
            case seal: String => Output.writeLineWithSeal(seal, message)
            case _ =>
        }
    }

    private def executeSHOW(statement: Statement): Unit = {
        val $list = statement.instance.asInstanceOf[SHOW]
        dh.show($list.rows.$eval(this).asInteger(20).toInt)
    }

    private def executeRUN(statement: Statement): Unit = {
        statement.instance.asInstanceOf[RUN].commandText.$restore(this).bash()
    }

    private def executeSELECT(statement: Statement): Unit = {
        val SQL = statement.sentence.$restore(this)
        val result = new SELECT(SQL).execute(this)
        RESULT = result.value
        ROWS = result.asTable.size

        if (dh.debugging) {
            Output.writeLine("                                                                        ")
            Output.writeLine(SQL.take(100))
            result.asTable.show()
        }
    }

    private def executeREQUEST(statement: Statement): Unit = {
        val $request = statement.instance.asInstanceOf[REQUEST]
        val url = $request.URL.$eval(this).asText
        val data = $request.data.$eval(this).asText
        val http: Http =
                        $request.method match {
                            case "POST" => Http.POST(url, data)
                            case "PUT" => Http.PUT(url, data)
                            case "DELETE" => Http.DELETE(url, data)
                            case _ => Http.GET(url)
                        }
        if ($request.header.nonEmpty) {
            for ((k, v) <- $request.header) {
                http.setHeader(k, v)
            }
        }

        dh.openJson(http.request())
    }

    private def executePARSE(statement: Statement): Unit = {
        RESULT = dh.parseTable(statement.instance.asInstanceOf[PARSE].path.$eval(this).asText)
        ROWS = RESULT.asInstanceOf[DataTable].size

        RESULT.asInstanceOf[DataTable].show()
    }

    private def executeDEBUG(statement: Statement): Unit = {
        val $debug = statement.instance.asInstanceOf[DEBUG]
        dh.debug($debug.switch.$eval(this).asBoolean(false))
    }

    private def execute(statements: ArrayBuffer[Statement]): Unit = {
        breakable {
            for (statement <- statements) {
                if (EXECUTOR.contains(statement.caption)) {
                    EXECUTOR(statement.caption)(statement)
                }
                else if (NON_QUERY_CAPTIONS.contains(statement.caption)) {
                    val SQL = statement.sentence.$restore(this)
                    AFFECTED = dh.executeNonQuery(SQL)

                    if (dh.debugging) {
                        Output.writeLine("                                                                        ")
                        Output.writeLine(SQL.take(100))
                        Output.writeLine("------------------------------------------------------------------------")
                        Output.writeLine(s"$AFFECTED row(s) affected. ")
                    }
                }
                else if (statement.caption == "CONTINUE") {
                    if (executeCONTINUE(statement)) {
                        break
                    }
                }
                else if (statement.caption == "EXIT") {
                    if (executeEXIT(statement)) {
                        break
                    }
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
                for (i <- FOR_VARIABLES.indices) {
                    if (FOR_VARIABLES(i).contains(name)) {
                        FOR_VARIABLES(i).set(name, value)
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
                for (i <- FOR_VARIABLES.indices) {
                    if (FOR_VARIABLES(i).contains(name)) {
                        cell = FOR_VARIABLES(i).get(name)
                        break
                    }
                }
            }

            breakable {
                for (i <- EXECUTING.indices) {
                    if (EXECUTING(i).containsVariable(name)) {
                        cell = EXECUTING(i).getVariable(name)
                        break
                    }
                }
            }
        }
        else if (symbol == "@") {
            //全局变量
            cell = GlobalVariable.get(name, this)

            //未找到忽略, MySQL的局部变量也是以 @ 开头
//            if (cell.invalid) {
//                throw new SQLExecuteException(s"Global variable $field is not found.")
//            }
        }

        cell
    }

    //传递参数和数据, Spring Boot的httpRequest参数
    def assign(args: Any): PSQL = {
        this.SQL = this.SQL.replaceArguments(args match {
            case ParameterMap(queries) => queries.asScala.map(kv => (kv._1, kv._2(0))).toMap
            case ArgumentMap(arguments) => arguments
            case queryString: String => queryString.toHashMap()
            case _ => Map[String, String]()
        })
        this
    }

    //设置单个变量的值
    def set(globalVariableName: String, value: Any): PSQL = {
        root.setVariable(globalVariableName, value)
        this
    }

    def $stash(value: DataCell): String = {
        this.values += value
        s"~value[${this.values.size - 1}]"
    }

    //运行但不关闭DataHub
    def $run(): PSQL = {
        this.parseAll()
        EXECUTING.push(root)
        this.execute(root.statements)
        dh.clear()

        this
    }

    def $return: Any = {
        if (RESULT != null) {
            RESULT
        }
        else if (AFFECTED > -1) {
            AFFECTED
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

    def show(): Unit = {
        val sentences = SQL.split(";")
        for (i <- sentences.indices) {
            Output.writeLine(i, ": ", sentences(i))
        }

        Output.writeLine("------------------------------------------------------------")
        this.root.show(0)
    }
}