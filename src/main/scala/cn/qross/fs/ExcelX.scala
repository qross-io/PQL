package cn.qross.fs

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.Files

import cn.qross.core.{DataTable, DataType}
import cn.qross.net.Email
import cn.qross.setting.Global
import cn.qross.fs.Path._

import org.apache.poi.hssf.record.cf.FontFormatting
import org.apache.poi.hssf.usermodel.{HSSFDataFormat, HSSFWorkbook}
import org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined
import org.apache.poi.ss.usermodel.{FillPatternType, HorizontalAlignment, VerticalAlignment, Workbook}
import org.apache.poi.ss.util.CellRangeAddress
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import scala.util.control.Breaks.{break, breakable}

object ExcelX {
    /*
    字号 1-300
    字形 301-400
    字体 401-500
    对齐 501-600
    边框 601-700
    背景 701-800
    数据类型 801-900
    颜色 >1001
    */

    val LastRow: Int = -1

    val FontName = "FontFamily"
    val FontSize = "FontSize"
    val FontStyle = "FontStyle"
    val FontColor = "FontColor"

    object Font {
        //FontSize
        val BigLarge: Short = 63
        val Large: Short = 54
        val Primary: Short = 42
        val SmallPrimary: Short = 36
        val BigOne: Short = 32
        val One: Short = 28
        val SmallOne: Short = 24
        val Two: Short = 21
        val SmallTwo: Short = 18
        val Three: Short = 16
        val SmallThree: Short = 15
        val Four: Short = 14
        val SmallFour: Short = 12
        val Five: Short = 11
        val SmallFive: Short = 9
        val Six: Short = 8
        val SmallSix: Short = 7
        val Seven: Short = 6
        val Eight: Short = 5

        //FontStyle
        val Bold: Short = 301
        val Italic: Short = 302
        val Underline: Short = 311
        val DoubleUnderline: Short = 312
        val AccountingUnderline: Short = 313
        val DoubleAccountingUnderline: Short = 314
        val Strikeout: Short = 315

        //fontName
        val Song = "宋体"
        val Kai = "楷体"
        val Hei = "黑体"
        val YaHei = "微软雅黑"
        val XinSong = "新宋体"
        val FangSong = "仿宋"
        val Arial = "Arial"
        val ArialBlack = "Arial Black"
        val TimesNewRoman = "Times New Roman"
        val CourierNew = "Courier New"
        val Tahoma = "Tahoma"
        val Verdana = "Verdana"
        val Calibri = "Calibri"
        val Consolas = "Consolas"
    }

    val Align = "Align"
    val VerticalAlign = "VerticalAlign"
    val ColumnWidth = "ColumnWidth"
    val RowHeight = "RowHeight"

    val BorderStyle = "BorderStyle"
    val BorderColor = "BorderColor"
    val BorderLeftStyle = "BorderLeftStyle"
    val BorderLeftColor = "BorderLeftColor"
    val BorderRightStyle = "BorderRightStyle"
    val BorderRightColor = "BorderRightColor"
    val BorderTopStyle = "BorderTopStyle"
    val BorderTopColor = "BorderTopColor"
    val BorderBottomStyle = "BorderBottomStyle"
    val BorderBottomColor = "BorderBottomColor"

    object Border {
        //BorderStyle
        val Dotted = org.apache.poi.ss.usermodel.BorderStyle.DOTTED
        val Hair = org.apache.poi.ss.usermodel.BorderStyle.HAIR
        val DashDotDot = org.apache.poi.ss.usermodel.BorderStyle.DASH_DOT_DOT
        val DashDot = org.apache.poi.ss.usermodel.BorderStyle.DASH_DOT
        val Dashed = org.apache.poi.ss.usermodel.BorderStyle.DASHED
        val Thin = org.apache.poi.ss.usermodel.BorderStyle.THIN
        val MediumDashDotDot = org.apache.poi.ss.usermodel.BorderStyle.MEDIUM_DASH_DOT_DOT
        val SlantedDashDot = org.apache.poi.ss.usermodel.BorderStyle.SLANTED_DASH_DOT
        val MediumDashDot = org.apache.poi.ss.usermodel.BorderStyle.MEDIUM_DASH_DOT
        val MediumDashed = org.apache.poi.ss.usermodel.BorderStyle.MEDIUM_DASHED
        val Medium = org.apache.poi.ss.usermodel.BorderStyle.MEDIUM
        val Thick = org.apache.poi.ss.usermodel.BorderStyle.THICK
        val Double = org.apache.poi.ss.usermodel.BorderStyle.DOUBLE
    }

    val BackgroundColor = "BackgroundColor"

    val DataFormat = "DataFormat"
    //val CustomDataFormat = "CustomDataFormat"

    //Align
    val Left: Short = 501
    val Center: Short = 502
    val Right: Short = 503
    val Top: Short = 504
    val Middle: Short = 505
    val Bottom: Short = 506

    val Auto: Short = 0

    object Color {

        val Indigo = HSSFColorPredefined.INDIGO
        val Grey80Percent = HSSFColorPredefined.GREY_80_PERCENT
        val Brown = HSSFColorPredefined.BROWN
        val OliveGreen = HSSFColorPredefined.OLIVE_GREEN
        val DarkGreen = HSSFColorPredefined.DARK_GREEN
        val SeaGreen = HSSFColorPredefined.SEA_GREEN
        val DarkTeal = HSSFColorPredefined.DARK_TEAL
        val Grey40Percent = HSSFColorPredefined.GREY_40_PERCENT
        val BlueGrey = HSSFColorPredefined.BLUE_GREY
        val Orange = HSSFColorPredefined.ORANGE
        val LightOrange = HSSFColorPredefined.LIGHT_ORANGE
        val Gold = HSSFColorPredefined.GOLD
        val LightBlue = HSSFColorPredefined.LIGHT_BLUE
        val Tan = HSSFColorPredefined.TAN
        val Lavender = HSSFColorPredefined.LAVENDER
        val Rose = HSSFColorPredefined.ROSE
        val PaleBlue = HSSFColorPredefined.PALE_BLUE
        val LightYellow = HSSFColorPredefined.LIGHT_YELLOW
        val LightGreen = HSSFColorPredefined.LIGHT_GREEN
        val SkyBlue = HSSFColorPredefined.SKY_BLUE
        val Aqua = HSSFColorPredefined.AQUA
        val Lime = HSSFColorPredefined.LIME
        val LightCornflowerBlue = HSSFColorPredefined.LIGHT_CORNFLOWER_BLUE
        val RoyalBlue = HSSFColorPredefined.ROYAL_BLUE
        val Coral = HSSFColorPredefined.CORAL
        val Orchid = HSSFColorPredefined.ORCHID
        val LightTurquoise = HSSFColorPredefined.LIGHT_TURQUOISE
        val LemonChiffon = HSSFColorPredefined.LEMON_CHIFFON
        val Plum = HSSFColorPredefined.PLUM
        val CornflowerBlue = HSSFColorPredefined.CORNFLOWER_BLUE
        val Grey50Percent = HSSFColorPredefined.GREY_50_PERCENT
        val Grey25Percent = HSSFColorPredefined.GREY_25_PERCENT
        val Teal = HSSFColorPredefined.TEAL
        val Violet = HSSFColorPredefined.VIOLET
        val DarkYellow = HSSFColorPredefined.DARK_YELLOW
        val DarkBlue = HSSFColorPredefined.DARK_BLUE
        val DarkRed = HSSFColorPredefined.DARK_RED
        val Turquoise = HSSFColorPredefined.TURQUOISE
        val Pink = HSSFColorPredefined.PINK
        val BrightGreen = HSSFColorPredefined.BRIGHT_GREEN
        val Green = HSSFColorPredefined.GREEN
        val Yellow = HSSFColorPredefined.YELLOW
        val Blue = HSSFColorPredefined.BLUE
        val Red = HSSFColorPredefined.RED
        val White = HSSFColorPredefined.WHITE
        val Black = HSSFColorPredefined.BLACK

        def of(r: Int, g: Int, b: Int): Short = 0 //r * 1000000 + g * 1000 + b
        def of(hexColor: String): Short = 0
    }

    //DataFormat
    def Decimal(precision: Int = 2): String = {
        precision match {
            case 1 => "0.0"
            case 2 => "0.00"
            case 3 => "0.000"
            case 4 => "0.0000"
            case 5 => "0.00000"
            case 6 => "0.000000"
            case 7 => "0.0000000"
            case 8 => "0.00000000"
            case 9 => "0.000000000"
            case _ => "0.00"
        }
    }

    val Money: String = "0.0000"
    def Percentage(precision: Int = 2): String = {
        precision match {
            case 0 => "0%"
            case 1 => "0.0%"
            case 2 => "0.00%"
            case 3 => "0.000%"
            case 4 => "0.0000%"
            case 5 => "0.00000%"
            case _ => "0.00%"
        }
    }

    def DateTimeFormat(formatStyle: String): String = {
        formatStyle
    }



    /*
    def appendSheet(sheetName: String = "sheet1", initialRow: Int = 0, initialColumn: Int = 0): DataHub = {

        if (TABLE.nonEmptySchema) {
            EXCEL
                    .setInitialRow(initialRow)
                    .setInitialColumn(0)
                    .openSheet(sheetName)
                    .withoutHeaders()
                    .appendTable(TABLE)
        }


        if (pageSQLs.nonEmpty || blockSQLs.nonEmpty) {
            stream(table => {
                EXCEL
                        .setInitialRow(initialRow)
                        .setInitialColumn(0)
                        .openSheet(sheetName)
                        .withoutHeaders()
                        .appendTable(table)
                table.clear()
            })
        }

        dh.preclear()

        dh
    }

    def appendSheetWithHeader(sheetName: String = "sheet1", initialRow: Int = 0, initialColumn: Int = 0): DataHub = {

        if (TABLE.nonEmptySchema) {
            EXCEL
                    .setInitialRow(initialRow)
                    .setInitialColumn(0)
                    .openSheet(sheetName)
                    .withHeaders()
                    .appendTable(TABLE)
        }

        if (pageSQLs.nonEmpty || blockSQLs.nonEmpty) {
            stream(table => {
                EXCEL
                        .setInitialRow(initialRow)
                        .setInitialColumn(0)
                        .openSheet(sheetName)
                        .withHeaders()
                        .appendTable(table)
                table.clear()
            })
        }

        dh.preclear()
    }

    def writeCellValue(value: Any, sheetName: String = "sheet1", rowIndex: Int = 0, colIndex: Int = 0): DataHub = {
        EXCEL.openSheet(sheetName).setCellValue(value, rowIndex, colIndex).setInitialRow(rowIndex).setInitialColumn(colIndex)
        this
    }

    def writeSheet(sheetName: String = "sheet1", initialRow: Int = 0, initialColumn: Int = 0): DataHub = {
        EXCEL
                .setInitialRow(initialRow)
                .setInitialColumn(0)
                .openSheet(sheetName)
                .withoutHeaders()
                .writeTable(TABLE)

        dh.preclear()

        this
    }

    def writeSheetWithHeader(sheetName: String = "sheet1", initialRow: Int = 0, initialColumn: Int = 0): DataHub = {
        EXCEL
                .setInitialRow(initialRow)
                .setInitialColumn(0)
                .openSheet(sheetName)
                .withHeaders()
                .writeTable(TABLE)

        dh.preclear()

        this
    }

    def setRegionStyle(rows: (Int, Int), cols: (Int, Int), styles: (String, Any)*): DataHub = {
        EXCEL.setStyle(rows, cols, 1, styles: _*)
        this
    }

    def withCellStyle(styles: (String, Any)*): DataHub = {
        EXCEL.setCellStyle(EXCEL.getInitialRow, EXCEL.getInitialColumn, styles: _*)
        this
    }

    def withHeaderStyle(styles: (String, Any)*): DataHub = {
        EXCEL.setRowStyle(
            EXCEL.getInitialRow,
            EXCEL.getInitialColumn -> (EXCEL.getInitialColumn + TABLE.getFields.size - 1),
            styles: _*)

        this
    }

    def withRowStyle(styles: (String, Any)*): DataHub = {
        EXCEL.setRowsStyle(
            EXCEL.getActualInitialRow -> (EXCEL.getActualInitialRow + TABLE.count() - 1),
            EXCEL.getInitialColumn -> (EXCEL.getInitialColumn + TABLE.getFields.size - 1),
            styles: _*)

        this
    }

    def withAlternateRowStyle(styles: (String, Any)*): DataHub = {
        EXCEL.setAlternateRowsStyle(
            EXCEL.getActualInitialRow -> (EXCEL.getActualInitialRow + TABLE.count() - 1),
            EXCEL.getInitialColumn -> (EXCEL.getInitialColumn + TABLE.getFields.size - 1),
            styles: _*
        )
        this
    }

    def mergeCells(rows: (Int, Int), cols: (Int, Int)): DataHub = {
        EXCEL.mergeRegion(rows, cols)
        dh
    }

    def removeMergeRegion(firstRow: Int, firstColumn: Int): DataHub = {
        EXCEL.removeMergedRegion(firstRow, firstColumn)
        dh
    }
    */

    //        def postExcelTo(url: String, data: String = ""): DataHub = {
    //            JSON = new Json(HttpClient.postFile(url, EXCEL.path, data))
    //            this
    //        }

    //actualInitialRow - append
    //def withStyle - initialRow + 1, TABLE.rows.size, initialColumn, TABLE.columns.size
    //def withAlternateStyle - initialRow + 2, TABLE.rows.size, initialColumn, TABLE.columns.size
    //def withHeaderStyle - initialRow, initialRow, initialColumn, , TABLE.columns.size
    //def mergeCells

    //    def attachExcelToEmail(title: String): DataHub = {
    //        EMAIL = new Email(title)
    //        EMAIL.attach(EXCEL.fileName)
    //        this
    //    }

}

class ExcelX(val fileName: String) {

    private var templatePath: String = ""
    private var sheetName: String = "sheet1"
    private var header: Boolean = true
    private var initialRow: Int = 0
    private var actualInitialRow: Int = 0
    private var initialColumn: Int = 0

    private val FASHION = "2007+"
    private val CLASSIC = "2003-"
    private val WRONG = "not a excel file"

    val path: String = {if (fileName.contains(".")) fileName else fileName + ".xlsx"}.locate()
    val file = new File(path)

    def fromTemplate(templateName: String): ExcelX = {
        this.templatePath = templateName.replace("\\", "/")

        if (!this.templatePath.endsWith(".xlsx")) {
            this.templatePath += ".xlsx"
        }

//        if (!this.templatePath.startsWith("/") && !this.templatePath.contains(":/")) {
//            this.templatePath = Global.EXCEL_TEMPLATES_PATH + this.templatePath
//        }

        if (!new File(templatePath).exists()) {
            throw new Exception(s"Excel template '$templateName' does not exists.")
        }

        this.header = false

        this
    }



    def openSheet(sheetName: String): ExcelX = {
        this.sheetName = sheetName
        this
    }

    def withHeaders(): ExcelX = {
        this.header = true
        this
    }

    def withoutHeaders(): ExcelX = {
        this.header = false
        this
    }

    def setInitialRow(row: Int): ExcelX = {
        this.initialRow = row
        this.actualInitialRow = row
        this
    }

    def setInitialColumn(column: Int): ExcelX = {
        this.initialColumn = column
        this
    }

    def getInitialRow: Int = this.initialRow
    def getInitialColumn: Int = this.initialColumn
    def getActualInitialRow: Int = this.actualInitialRow

    def getWorkbook: Workbook = {

        val version: String =
            fileName.substring(fileName.lastIndexOf(".") + 1).toLowerCase() match {
                case "xls" => CLASSIC
                case "xlsx" => FASHION
                case _ => WRONG
            }

        if (version == WRONG) {
            throw new Exception("Wrong file type. You must specify an excel file.")
        }

        if (!file.exists() && templatePath != "") {
            Files.copy(new File(this.templatePath).toPath, file.toPath)
        }

        if (file.exists()) {
            val fis = new FileInputStream(file)
            val workbook = if (version == FASHION) {
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
            if (version == FASHION) {
                new SXSSFWorkbook()
            }
            else {
                new HSSFWorkbook()
            }
        }

    }

    def appendTable(table: DataTable): ExcelX = {
        save(table)
        this
    }

    def writeTable(table: DataTable): ExcelX = {
        save(table, false)
        this
    }

    private def save(table: DataTable, append: Boolean = true): ExcelX = {

        val workbook = this.getWorkbook

        var sheet = workbook.getSheet(sheetName)
        if (sheet == null) {
            sheet = workbook.createSheet(sheetName)
        }

        var startRow = initialRow
        if (header) {
            val head = sheet.createRow(startRow) // XSSFRow-2007 HSSFRow-2003
            //val cell = row.createCell(0) //XSSFCell-2007 HSSFCell-2003
            val labels = table.getLabelNames
            for (i <- labels.indices) {
                head.createCell(i).setCellValue(labels(i))
            }
            startRow += 1
        }

        //find empty row
        if (append) {
            while (sheet.getRow(startRow) != null) {
                startRow += 1
            }
        }

        actualInitialRow = startRow

        val rows = table.rows
        for (i <- rows.indices) {
            val row = sheet.createRow(i + startRow)
            val fields = rows(i).getFields
            val types = rows(i).getDataTypes
            for (j <- fields.indices) {
                val cell = row.createCell(j + initialColumn) //startColumn
                types(j) match {
                    case DataType.INTEGER => cell.setCellValue(rows(i).getLong(fields(j)))
                    case DataType.DECIMAL => cell.setCellValue(rows(i).getDouble(fields(j)))
                    case _ => cell.setCellValue(rows(i).getString(fields(j)))
                }
            }
        }

        //        for (i <- 0 until table.getFields.size) {
        //            sheet.autoSizeColumn(initialColumn + i)
        //        }

        val fos = new FileOutputStream(file) //can't not be true, or file will be wrong.
        workbook.write(fos)
        workbook.close()
        fos.close()

        this
    }

    def setCellValue(value: Any, rowIndex: Int = 0, colIndex: Int = 0) : ExcelX = {

        val workbook = this.getWorkbook

        var sheet = workbook.getSheet(sheetName)
        if (sheet == null) {
            sheet = workbook.createSheet(sheetName)
        }

        val cell = sheet.createRow(rowIndex).createCell(colIndex)
        if (value.isInstanceOf[Int] || value.isInstanceOf[Long] || value.isInstanceOf[Float] || value.isInstanceOf[Double] || value.isInstanceOf[Short]) {
            cell.setCellValue(value.asInstanceOf[Double])
        }
        else {
            cell.setCellValue(value.asInstanceOf[String])
        }

        val fos = new FileOutputStream(file) //can't not be true, or file will be wrong.
        workbook.write(fos)
        workbook.close()

        this
    }

    /*
    saveAsNewExcel("")
        .appendSheet("sheet1")
            .openExcelSheet(""sheet2)
                .setExcelStyle()
    * */

    def setStyle(rows: (Int, Int), cols: (Int, Int), rowStep: Int, styles: (String, Any)*): ExcelX = {

        val workbook = this.getWorkbook

        val sheet = workbook.getSheet(sheetName)
        if (sheet == null) {
            throw new Exception("Wrong sheet name")
        }

        val cellStyle = workbook.createCellStyle()
        val font = workbook.createFont()
        var rowHeight = 0
        var columnWidth = -1

        for ((name, value) <- styles) {
            name match {
                case ExcelX.FontName => font.setFontName(value.asInstanceOf[String])
                case ExcelX.FontSize => font.setFontHeightInPoints(
                    value match {
                        case size: Int => size.toShort
                        case size: Short => size
                        case _ => ExcelX.Font.Five
                    })
                case ExcelX.FontStyle =>
                    value match {
                        case ExcelX.Font.Bold => font.setBold(true)
                        case ExcelX.Font.Italic => font.setItalic(true)
                        case ExcelX.Font.Underline => font.setUnderline(FontFormatting.U_SINGLE)
                        case ExcelX.Font.DoubleUnderline => font.setUnderline(FontFormatting.U_DOUBLE)
                        case ExcelX.Font.AccountingUnderline => font.setUnderline(FontFormatting.U_SINGLE_ACCOUNTING)
                        case ExcelX.Font.DoubleAccountingUnderline => font.setUnderline(FontFormatting.U_DOUBLE_ACCOUNTING)
                        case ExcelX.Font.Strikeout => font.setStrikeout(true)
                        case _ =>
                    }
                case ExcelX.FontColor => font.setColor(value.asInstanceOf[HSSFColorPredefined].getIndex)
                case ExcelX.Align =>
                    value match {
                        case ExcelX.Left => cellStyle.setAlignment(HorizontalAlignment.LEFT)
                        case ExcelX.Center => cellStyle.setAlignment(HorizontalAlignment.CENTER)
                        case ExcelX.Right => cellStyle.setAlignment(HorizontalAlignment.RIGHT)
                        case _ =>
                    }
                case ExcelX.VerticalAlign =>
                    value match {
                        case ExcelX.Top => cellStyle.setVerticalAlignment(VerticalAlignment.TOP)
                        case ExcelX.Middle => cellStyle.setVerticalAlignment(VerticalAlignment.CENTER)
                        case ExcelX.Bottom => cellStyle.setVerticalAlignment(VerticalAlignment.BOTTOM)
                        case _ =>
                    }
                case ExcelX.ColumnWidth =>
                    columnWidth = value match {
                        case w: Int => w.toShort
                        case w: Short => w
                        case _ => ExcelX.Auto
                    }

                case ExcelX.RowHeight => rowHeight = value.asInstanceOf[Int].toShort
                case ExcelX.BorderStyle =>
                    cellStyle.setBorderBottom(value.asInstanceOf[org.apache.poi.ss.usermodel.BorderStyle])
                    cellStyle.setBorderLeft(value.asInstanceOf[org.apache.poi.ss.usermodel.BorderStyle])
                    cellStyle.setBorderRight(value.asInstanceOf[org.apache.poi.ss.usermodel.BorderStyle])
                    cellStyle.setBorderTop(value.asInstanceOf[org.apache.poi.ss.usermodel.BorderStyle])
                case ExcelX.BorderColor =>
                    cellStyle.setBottomBorderColor(value.asInstanceOf[HSSFColorPredefined].getIndex)
                    cellStyle.setLeftBorderColor(value.asInstanceOf[HSSFColorPredefined].getIndex)
                    cellStyle.setRightBorderColor(value.asInstanceOf[HSSFColorPredefined].getIndex)
                    cellStyle.setTopBorderColor(value.asInstanceOf[HSSFColorPredefined].getIndex)
                case ExcelX.BorderBottomStyle => cellStyle.setBorderBottom(value.asInstanceOf[org.apache.poi.ss.usermodel.BorderStyle])
                case ExcelX.BorderLeftStyle => cellStyle.setBorderLeft(value.asInstanceOf[org.apache.poi.ss.usermodel.BorderStyle])
                case ExcelX.BorderRightStyle => cellStyle.setBorderRight(value.asInstanceOf[org.apache.poi.ss.usermodel.BorderStyle])
                case ExcelX.BorderTopStyle => cellStyle.setBorderTop(value.asInstanceOf[org.apache.poi.ss.usermodel.BorderStyle])
                case ExcelX.BorderBottomColor => cellStyle.setBottomBorderColor(value.asInstanceOf[HSSFColorPredefined].getIndex)
                case ExcelX.BorderLeftColor => cellStyle.setLeftBorderColor(value.asInstanceOf[HSSFColorPredefined].getIndex)
                case ExcelX.BorderRightColor => cellStyle.setRightBorderColor(value.asInstanceOf[HSSFColorPredefined].getIndex)
                case ExcelX.BorderTopColor => cellStyle.setTopBorderColor(value.asInstanceOf[HSSFColorPredefined].getIndex)
                case ExcelX.BackgroundColor =>
                    cellStyle.setFillForegroundColor(value.asInstanceOf[HSSFColorPredefined].getIndex)
                    cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND)
                case ExcelX.DataFormat => cellStyle.setDataFormat(HSSFDataFormat.getBuiltinFormat(value.asInstanceOf[String]))
                //case CustomDataFormat => format = value.asInstanceOf[String]
                case _ =>
            }
        }
        cellStyle.setFont(font)
        //        if (style._4 != "") {
        //            //custom data format
        //            style._1.setDataFormat(workbook.createDataFormat().getFormat(style._4))
        //        }

        var (rowIndex, lastRow) = rows
        var (firstColumn, lastColumn) = cols

        if (lastRow == ExcelX.LastRow) {
            lastRow = sheet.getLastRowNum
        }

        if (rowIndex == lastRow && firstColumn == lastColumn) {
            //only one cell
            for (i <- 0 until sheet.getNumMergedRegions) {
                val region = sheet.getMergedRegion(i)
                if (region.isInRange(rowIndex, firstColumn)) {
                    rowIndex = region.getFirstRow
                    lastRow = region.getLastRow
                    firstColumn = region.getFirstColumn
                    lastColumn = region.getLastColumn
                }
            }
        }

        while (rowIndex <= lastRow) {
            val row = sheet.getRow(rowIndex)
            if (row != null) {
                if (rowHeight > 0) {
                    row.setHeightInPoints(rowHeight)
                }
                for (j <- firstColumn to lastColumn) {
                    val cell = row.getCell(j)
                    if (cell != null) {
                        cell.setCellStyle(cellStyle)
                    }
                    else {
                        row.createCell(j).setCellStyle(cellStyle)
                    }
                }
            }

            rowIndex += rowStep
        }

        //columnWidth
        for (j <- cols._1 to cols._2) {
            if (columnWidth > 0) {
                sheet.setColumnWidth(j, columnWidth * 256)
            }
            else if (columnWidth == 0) {
                val before = sheet.getColumnWidth(j)
                sheet.autoSizeColumn(j, true)
                val after = sheet.getColumnWidth(j)
                if (after < before) {
                    sheet.setColumnWidth(j, after * 2)
                }
            }
        }

        val fos = new FileOutputStream(file)
        workbook.write(fos)
        workbook.close()

        this
    }

    def setCellStyle(rowIndex: Int, colIndex: Int, styles: (String, Any)*): ExcelX = {
        setStyle(rowIndex -> rowIndex, colIndex -> colIndex, 1, styles: _*)
    }

    def setRowStyle(row: Int, cols: (Int, Int), styles: (String, Any)*): ExcelX = {
        setStyle(row -> row, cols._1 -> cols._2, 1, styles: _*)
    }

    def setRowsStyle(rows: (Int, Int), cols: (Int, Int), styles: (String, Any)*): ExcelX = {
        setStyle(rows._1 -> rows._2, cols._1 -> cols._2, 1, styles: _*)
    }

    def setAlternateRowsStyle(rows: (Int, Int), cols: (Int, Int), styles: (String, Any)*): ExcelX = {
        setStyle(rows._1 -> rows._2, cols._1 -> cols._2, 2, styles: _*)
        this
    }

    def setColumnStyle(initialRow: Int, colIndex: Int, styles: (String, Short)*): ExcelX = {
        setStyle(initialRow -> ExcelX.LastRow, colIndex -> colIndex, 1, styles: _*)
        this
    }

    def setColumnsStyle(initialRow: Int, cols: (Int, Int), styles: (String, Short)*): ExcelX = {
        setStyle(initialRow -> ExcelX.LastRow, cols._1 -> cols._2, 1, styles: _*)
        this
    }

    def mergeRegion(rows: (Int, Int), cols: (Int, Int)): ExcelX = {

        val workbook = this.getWorkbook

        val sheet = workbook.getSheet(sheetName)
        if (sheet == null) {
            throw new Exception("Wrong sheet name")
        }

        //val cellStyle = sheet.getRow(rows._1).getCell(cols._1).getCellStyle
        //for ()

        sheet.addMergedRegion(new CellRangeAddress(rows._1, rows._2, cols._1, cols._2))
        //sheet.addMergedRegionUnsafe(new CellRangeAddress(rows._1, rows._2, cols._1, cols._2))

        val fos = new FileOutputStream(file)
        workbook.write(fos)
        workbook.close()

        this
    }

    def removeMergedRegion(firstRow: Int, firstColumn: Int): ExcelX = {

        val workbook = this.getWorkbook

        val sheet = workbook.getSheet(sheetName)
        if (sheet == null) {
            throw new Exception("Wrong sheet name")
        }

        var index = -1
        breakable {
            sheet.getMergedRegions.forEach(region => {
                index += 1
                if (region.getFirstRow == firstRow && region.getFirstColumn == firstColumn) {
                    break
                }
            })
        }
        if (index > -1) {
            sheet.removeMergedRegion(index)
        }

        val fos = new FileOutputStream(file)
        workbook.write(fos)
        workbook.close()
        fos.close()

        this
    }

    def attachToEmail(title: String): Email = {
        Email.write(title).attach(path)
    }

    def readTable(sheetName: String = "sheet1", startRow: Int = 0, startColumn: Int = 0)(fields: (String, DataType)*): DataTable = {

        val fis = new FileInputStream(new File(path))
        //val workbook: Workbook = if (version == FASHION) new XSSFWorkbook(fis) else new HSSFWorkbook(fis)
        new DataTable()

        /*
        //获取工作表
        val sheet = workbook.getSheet(sheetName)
        //获取行,行号作为参数传递给getRow方法,第一行从0开始计算
        val row = sheet.getRow(0)

        //获取单元格,row已经确定了行号,列号作为参数传递给getCell,第一列从0开始计算
        val cell = row.getCell(2)
        //设置单元格的值,即C1的值(第一行,第三列)
        val cellValue = cell.getStringCellValue
        System.out.println("第一行第三列的值是" + cellValue)

        //sheet.getPhysicalNumberOfRows
        //sheet.getLastRowNum
        //row.getLastCellNum

        workbook.close

        DataTable()
        */
    }
}
