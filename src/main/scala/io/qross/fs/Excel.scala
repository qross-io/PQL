package io.qross.fs

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.Files

import io.qross.core._
import io.qross.exception.{IncorrectDataSourceException, SQLParseException, UnsupportedSentenceException}
import io.qross.net.Email
import io.qross.setting.Global
import io.qross.fs.Path._
import io.qross.pql.Patterns._
import io.qross.ext.TypeExt._
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.pql.{Patterns, Syntax}
import io.qross.pql.Solver._
import org.apache.poi.hssf.record.cf.FontFormatting
import org.apache.poi.hssf.usermodel.{HSSFDataFormat, HSSFWorkbook}
import org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined
import org.apache.poi.ss.usermodel._
import org.apache.poi.ss.util.CellRangeAddress
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.springframework.web.context.request.{RequestContextHolder, ServletRequestAttributes}

import scala.util.{Success, Try}
import scala.util.control.Breaks._

object Excel {

    val FASHION = "2007+"
    val CLASSIC = "2003-"
    val WRONG = "NaE" //not an excel file

    def open(fileName: String): Excel = {
        new Excel(fileName)
    }

    def getColumnNumber(column: String): Int = {
        if ("""^\d+$""".r.test(column)) {
            column.toInt - 1
        }
        else if ("""(?i)^[A-Z]+$""".r.test(column)) {
            var n = 0
            for (i <- column.indices) {
                n += (column.charAt(i).toInt - 64) * Math.pow(26, column.length - 1 - i).toInt
            }
            n - 1
        }
        else {
            throw new IllegalArgumentException("Wrong excel column. All its char must be letter or number.")
        }
    }

    implicit class DataHub$Excel(val dh: DataHub) {

        def ZIP: Zip = dh.pick[Zip]("ZIP").orNull

        def openExcel(fileNameOrFullPath: String): DataHub = {
            dh.openSource(fileNameOrFullPath, new Excel(fileNameOrFullPath).debug(dh.debugging))
        }

        def saveToExcel(fileNameOrFullPath: String): DataHub = {
            val excel = new Excel(fileNameOrFullPath).debug(dh.debugging)
            dh.saveToDestination(fileNameOrFullPath, excel)
            if (dh.slots("ZIP")) {
                ZIP.addFile(excel.path)
            }
            dh
        }

        def saveAsExcel(fileNameOrFullPath: String): DataHub = {
            fileNameOrFullPath.delete()
            saveToExcel(fileNameOrFullPath)
        }

        def saveAsStreamExcel(fileName: String): DataHub = {
            dh.saveToDestination(fileName, new Excel(fileName).debug(dh.debugging).setAutoCommit(false).saveAsStream())
        }

        def useTemplate(templateName: String): DataHub = {
            if (templateName != "") {
                dh.getDestination match {
                    case excel: Excel => excel.useTemplate(templateName)
                    case _ => throw new IncorrectDataSourceException("Must save an excel file first.")
                }
            }
            dh
        }

        def attachExcelToEmail(title: String) : DataHub = {
            dh.getDestination match {
                case excel: Excel =>
                    dh.plug("EMAIL", new Email(title))
                        .pick[Email]("EMAIL").orNull
                        .attach(excel.path)
                case _ => throw new IncorrectDataSourceException("Must save an excel file first.")
            }
            dh
        }
    }
}

class Excel(val fileName: String) {

    var output: String = "file" // file or stream
    var autoCommit: Boolean = true //是否自动关闭文件, 在单线程模式下, 查询或修改一次即关闭, 在多线程模式下或流模式下需要手动关闭

    var templatePath: String = ""

    val path: String = {if (fileName.contains(".")) fileName else fileName + ".xlsx"}.locate()
    private val file = new File(path)

    private var closed: Boolean = true
    private var $workbook: Workbook = _

    //是否启用调试
    private var DEBUG = false
    def debugging: Boolean = DEBUG

    def debug(enabled: Boolean = true): Excel = {
        DEBUG = enabled
        this
    }

    def saveAsStream(): Excel = {
        output = "stream"
        this
    }

    def saveAsFile(): Excel = {
        output = "file"
        this
    }

    def setAutoCommit(toCommit: Boolean): Excel = {
        autoCommit = toCommit
        this
    }

    def open(fileNameOrFullPath: String): Excel = {
        this
    }

    def useTemplate(fileNameOrFullPathOrTemplateName: String): Excel = {

        templatePath = fileNameOrFullPathOrTemplateName

        if (!templatePath.endsWith(".xlsx")) {
            if (JDBC.hasQrossSystem) {
                DataSource.QROSS.querySingleValue("SELECT template_path FROM qross_excel_templates WHERE template_name=?", this.templatePath).data match {
                    case Some(tplPath) => templatePath = tplPath.toString
                    case None => templatePath += ".xlsx"
                }
            }
        }

        templatePath = templatePath.toPath

        if (!this.templatePath.startsWith("/") && !this.templatePath.contains(":/")) {
            this.templatePath = Global.EXCEL_TEMPLATES_PATH + this.templatePath
        }

        if (!new File(templatePath).exists()) {
            throw new Exception(s"Excel template '$templatePath' does not exists.")
        }

        this
    }

    def useDefaultTemplate(): Excel = {
        useTemplate(Global.EXCEL_DEFAULT_TEMPLATE)
        this
    }

    private def workbook: Workbook = {
        if ($workbook == null || closed) {
            val version: String =
                fileName.substring(fileName.lastIndexOf(".") + 1).toLowerCase() match {
                    case "xls" => Excel.CLASSIC
                    case "xlsx" => Excel.FASHION
                    case _ => throw new IllegalArgumentException("Wrong file type. You must specify an excel file.")
                }

            //从模板新建
            if (!file.exists() && templatePath != "") {
                Files.copy(new File(this.templatePath).toPath, file.toPath)
            }

            closed = false

            $workbook = {
                if (file.exists()) {
                    //打开
                    val fis = new FileInputStream(file)
                    val workbook = if (version == Excel.FASHION) {
                        new XSSFWorkbook(fis)
                        //new HSSFWorkbook(fis)
                    }
                    else {
                        new HSSFWorkbook(fis)
                    }
                    fis.close()
                    workbook
                }
                else {
                    //新建
                    if (version == Excel.FASHION) {
                        new SXSSFWorkbook()
                    }
                    else {
                        new HSSFWorkbook()
                    }
                }
            }
        }

        $workbook
    }

    def createSheet(sheetName: String): Unit = {
        val sheet = workbook.getSheet(sheetName)
        if (sheet == null) {
            workbook.createSheet(sheetName)
        }

        commitIfAuto()
    }

    def cloneSheet(sheetName: String, newSheetName: String): Unit = {
        val sheetNum = workbook.getSheetIndex(sheetName)
        if (sheetNum > -1) {
            workbook.cloneSheet(sheetNum)
            workbook.setSheetName(workbook.getNumberOfSheets - 1, newSheetName)
        }
        else {
            throw new IllegalArgumentException("Wrong sheet name: " + sheetName)
        }

        commitIfAuto()
    }

    def cloneSheet(sheetNum: Int, newSheetName: String): Unit = {
        if (sheetNum > -1) {
            workbook.cloneSheet(sheetNum)
            workbook.setSheetName(workbook.getNumberOfSheets - 1, newSheetName)
        }
        else {
            throw new IllegalArgumentException("Wrong sheet index: " + sheetNum)
        }

        commitIfAuto()
    }

    def dropSheet(sheetName: String): Unit = {
        val sheetNum = workbook.getSheetIndex(sheetName)
        if (sheetNum > -1) {
            workbook.removeSheetAt(sheetNum)
        }
        else {
            throw new IllegalArgumentException("Wrong sheet name: " + sheetName)
        }
    }

    def dropSheet(sheetNum: Int): Unit = {
        if (sheetNum > -1) {
            workbook.removeSheetAt(sheetNum)
        }
        else {
            throw new IllegalArgumentException("Wrong sheet index: " + sheetNum)
        }

        commitIfAuto()
    }

    def renameSheet(sheetName: String, newSheetName: String): Unit = {
        val sheetNum = workbook.getSheetIndex(sheetName)
        if (sheetNum > -1) {
            workbook.setSheetName(sheetNum, newSheetName)
        }
        else {
            throw new IllegalArgumentException("Wrong sheet name: " + sheetName)
        }

        commitIfAuto()
    }

    def renameSheet(sheetNum: Int, newSheetName: String): Unit = {
        if (sheetNum > -1) {
            workbook.setSheetName(sheetNum, newSheetName)
        }
        else {
            throw new IllegalArgumentException("Wrong sheet index: " + sheetNum)
        }

        commitIfAuto()
    }

    def moveSheetTo(sheetName: String, position: Int): Unit = {
        val length = workbook.getNumberOfSheets
        if (position > -1) {
            workbook.setSheetOrder(sheetName, if (position < length) position else length - 1)
        }
        else {
            throw new IllegalArgumentException("Wrong sheet position: " + position)
        }

        commitIfAuto()
    }

    def moveSheetBefore(sheetName: String, nextSheetName: String): Unit = {
        val origin = workbook.getSheetIndex(sheetName)
        val position = workbook.getSheetIndex(nextSheetName)

        if (origin > position && position > -1) {
            workbook.setSheetOrder(sheetName, position)
        }
        else if (origin < position && origin > -1) {
            workbook.setSheetOrder(sheetName, position - 1)
        }
        else if (origin < 0) {
            throw new IllegalArgumentException("Wrong sheet name: " + sheetName)
        }
        else if (position < 0) {
            throw new IllegalArgumentException("Wrong sheet name: " + nextSheetName)
        }

        commitIfAuto()
    }

    def moveSheetAfter(sheetName: String, prevSheetName: String): Unit = {
        val origin = workbook.getSheetIndex(sheetName)
        val position = workbook.getSheetIndex(prevSheetName)

        if (origin > position && position > -1) {
            workbook.setSheetOrder(sheetName, position + 1)
        }
        else if (origin < position && origin > -1) {
            workbook.setSheetOrder(sheetName, position)
        }
        else if (origin < 0) {
            throw new IllegalArgumentException("Wrong sheet name: " + sheetName)
        }
        else if (position < 0) {
            throw new IllegalArgumentException("Wrong sheet name: " + prevSheetName)
        }

        commitIfAuto()
    }

    //返回最新的游标位置
    def insert(sentence: String): Int = {
        insert(sentence, new DataTable(new DataRow()))
    }

    //插入一行
    def insert(sentence: String, dataRow: DataRow): Int = {
        insert(sentence, new DataTable(dataRow))
    }

    //插入多行, 返回受影响的记录数
    //INSERT INTO sheet1 ROW 2 (A1, B1, C1) VALUES ('姓名', '年龄', '分数');
    def insert(sentence: String, table: DataTable): Int = {

        if (DEBUG) {
            println("EXCEL INSERT # " + sentence)
        }

        val plan = Syntax("INSERT").plan(
            $INSERT$INTO.findFirstIn(sentence) match {
                case Some(capital) => sentence.takeAfter(capital).trim()
                case None => sentence
            })

        //暂时不判断格式正确性

        var sheet: Sheet = null
        var cursor = 0 //startRow and cursorRow
        val columns = plan.multiArgs("").map(_.removeQuotes())

        val sheetName = plan.oneArgs("INTO")
        sheet = workbook.getSheet(sheetName)
        if (sheet == null) {
            sheet = workbook.createSheet(sheetName)
        }

        if (plan.contains("ROW")) {
            Try(plan.oneArgs("ROW").toInt) match {
                case Success(v) =>
                    if (v > cursor) {
                        cursor = v - 1
                    }
                case _ =>
            }
        }

        //开始写入
        while (sheet.getRow(cursor) != null) {
            cursor += 1
        }

        var rows = 0
        table.foreach(dataRow => {

            for (values <- plan.moreArgs(dataRow, "VALUES")) {

                if (DEBUG && rows < 10) {
                    print("\t")
                    println("(" + values.mkString(",") + ")")
                }

                val row = sheet.createRow(cursor)
                if (columns.length != values.length) {
                    throw new SQLParseException("Fields amount must equals values amount. " + sentence)
                }
                else if (columns.isEmpty) {
                    throw new SQLParseException("No data to insert. " + sentence)
                }

                for (i <- values.indices) {
                    val cell = row.createCell(Excel.getColumnNumber(columns(i)))
                    val value = values(i)
                    if (value.bracketsWith("'", "'") || value.bracketsWith("\"", "\"")) {
                        cell.setCellValue(value.removeQuotes())
                    }
                    else if ($INTEGER.test(value)) {
                        cell.setCellValue(value.toLong)
                    }
                    else if ($DECIMAL.test(value)) {
                        cell.setCellValue(value.toDouble)
                    }
                    else {
                        cell.setCellValue(value)
                    }
                }

                cursor += 1
                rows += 1
            }
        })

        commitIfAuto()

        rows
    }

    def delete(sentence: String): Int = {
        0
    }

    def delete(sentence: String, table: DataTable): Int = {
        0
    }

    def update(sentence: String): Int = {
        0
    }

    def update(sentence: String, table: DataTable): Int = {
        0
    }

    def select(sentence: String, values: Any*): DataTable = {
        new DataTable()
    }

    def executeNonQuery(nonQuerySQL: String): Int = {
        nonQuerySQL.trim().takeBefore("\\s".r).toUpperCase() match {
            case "INSERT" => insert(nonQuerySQL)
            case "UPDATE" => update(nonQuerySQL)
            case "DELETE" => delete(nonQuerySQL)
            case "DROP" =>
                """(?i)^DROP\s+(SHEET|TABLE)\s""".r.findFirstIn(nonQuerySQL) match {
                    case Some(sheet) => dropSheet(nonQuerySQL.takeAfter(sheet).trim())
                    case _ => dropSheet(nonQuerySQL.takeAfter("\\s").trim())
                }
                0
            case _ => throw new UnsupportedSentenceException("Unsupported sentence at executeNonQuery method in Excel: " + nonQuerySQL)
        }
    }

    def tableUpdate(nonQuerySQL: String, table: DataTable): Int = {
        nonQuerySQL.trim().takeBefore("\\s".r).toUpperCase() match {
            case "INSERT" => insert(nonQuerySQL, table)
            case "UPDATE" => update(nonQuerySQL, table)
            case "DELETE" => delete(nonQuerySQL, table)
            case _ => throw new UnsupportedSentenceException("Unsupported sentence at tableUpdate method in Excel: " + nonQuerySQL)
        }
    }

    def tableSelect(sentence: String, table: DataTable): DataTable = {
        //pass
        new DataTable()
    }

    //EDIT Sheet1 SET A1, A2 STYLE 'styles' MERGE A1, B2 SPLIT A3;
    //EDIT Sheet1 SET A, B CELL STYLE 'styles' WHERE A>0.1;
    //EDIT Sheet1 SET A, B ROW STYLE 'styles' WHERE A>0.1;
    def edit(sentence: String): Excel = {
        this
    }

    //批量提交, 多线程写入时需要
    def prepare(sentence: String): Unit = ???
    def put(table: DataTable): Unit = ???

    def commitIfAuto(): Unit = {
        if (autoCommit) {
            close()
        }
    }

    def attachToEmail(title: String): Email = {
        //须在关闭之后执行
        close()
        Email.write(title).attach(path)
    }

    def close(): Unit = {
        if (!closed) {
            if (output == "file") {
                val fos = new FileOutputStream(file)
                $workbook.write(fos)
                fos.close()
            }
            else {
                //必须在web环境下才可以
                val attributes: ServletRequestAttributes = RequestContextHolder.getRequestAttributes.asInstanceOf[ServletRequestAttributes]
                if (attributes != null) {
                    val response = attributes.getResponse
                    response.setHeader("Content-type", "application/octet-stream")
                    response.setCharacterEncoding("UTF-8")
                    response.setHeader("Content-disposition", "attachment;filename=\"" + new String(path.takeAfterLast("/").getBytes("UTF-8"), "ISO-8859-1") + "\"")
                    response.flushBuffer()
                    $workbook.write(response.getOutputStream)
                }
            }

            $workbook.close()
            $workbook = null
            closed = true
        }
    }
}

