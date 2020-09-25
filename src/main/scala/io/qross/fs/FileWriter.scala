package io.qross.fs

import java.io._

import io.qross.core.DataTable
import io.qross.fs.Path._
import io.qross.net.Json
import io.qross.setting.Global
import org.springframework.web.context.request.{RequestContextHolder, ServletRequestAttributes}

import scala.collection.mutable.ArrayBuffer

class FileWriter(val file: File, val format: Int, val outputType: String, deleteIfExists: Boolean) {

    def this(file: File) {
        this(file, TextFile.TXT, TextFile.FILE, false)
    }

    def this(filePath: String) {
        this(new File(filePath.locate()), TextFile.TXT, TextFile.FILE, false)
    }

    def this(file: File, format: Int) {
        this(file, format, TextFile.FILE, false)
    }

    def this(filePath: String, format: Int) {
        this(new File(filePath.locate()), format, TextFile.FILE, false)
    }

    def this(file: File, outputType: String) {
        this(file, TextFile.TXT, outputType, false)
    }

    def this(filePath: String, outputType: String) {
        this(new File(filePath.locate()), TextFile.TXT, outputType, false)
    }

    def this(file: File, deleteIfExists: Boolean) {
        this(file, TextFile.TXT, TextFile.FILE, deleteIfExists)
    }

    def this(filePath: String, deleteIfExists: Boolean) {
        this(new File(filePath.locate()), TextFile.TXT, TextFile.FILE, deleteIfExists)
    }

    def this(file: File, format: Int, outputType: String) {
        this(file, format, outputType, false)
    }

    def this(filePath: String, format: Int, outputType: String) {
        this(new File(filePath.locate()), format, outputType, false)
    }

    def this(file: File, format: Int, deleteIfExists: Boolean) {
        this(file, format, TextFile.FILE, deleteIfExists)
    }

    def this(filePath: String, format: Int, deleteIfExists: Boolean) {
        this(new File(filePath.locate()), format, TextFile.FILE, deleteIfExists)
    }

    def this(filePath: String, format: Int, outputType: String, deleteIfExists: Boolean) {
        this(new File(filePath.locate()), format, outputType, deleteIfExists)
    }

    private var delimiter = "," //default delimiter when you write a collection
    private var append = false

    private val writer: BufferedWriter  = {
        val attributes: ServletRequestAttributes = {
            if (outputType == TextFile.STREAM) {
                RequestContextHolder.getRequestAttributes.asInstanceOf[ServletRequestAttributes]
            }
            else {
                null
            }
        }

        val fos: OutputStream = {
            if (attributes != null) {
                val response = attributes.getResponse
                response.setHeader("Content-type", "application/octet-stream")
                response.setCharacterEncoding(Global.CHARSET)
                response.setHeader("Content-disposition", "attachment;filename=\"" + new String(file.getName.getBytes("UTF-8"), "ISO-8859-1") + "\"")
                response.flushBuffer()
                response.getOutputStream
            }
            else {
                //exists, keep, >0B -> append = true
                if (file.exists()) {
                    if (deleteIfExists) {
                        file.delete()
                    }
                    else if (file.length() > 0) {
                        append = true
                    }
                }
                if (!file.exists()) {
                    file.getParentFile.mkdirs()
                    file.createNewFile()
                }

                new FileOutputStream(file, true)
            }
        }

        if (!append && file.getName.toLowerCase().endsWith(".csv")) {
            fos.write(Array[Byte](0xEF.toByte, 0xBB.toByte, 0xBF.toByte))
        }

        new BufferedWriter(new OutputStreamWriter(fos, Global.CHARSET))
    }
    //PrintWriter and FileWriter is advanced implements of OutputStreamWriter
    //private val output = new PrintWriter(file)
    //private val output = new BufferedWriter(new java.io.FileWriter(file))

    def delimit(delimiter: String): FileWriter = {
        this.delimiter = delimiter
        this
    }

    def  writeTable(table: DataTable, withHeaders: Boolean = true): FileWriter = {
        if (format != TextFile.JSON) {
            if (withHeaders && !append) {
                writeLine(table.getLabelNames.mkString(delimiter))
            }
            table.foreach(row => {
                writeLine(row.getValues.mkString(delimiter))
            })
        }
        else {
            table.foreach(row => {
                writeLine(row.toString)
            })
        }
        this
    }

    def writeTable(table: DataTable): FileWriter = {
        writeTable(table, true)
    }

    def write(content: String): FileWriter = {
        writer.append(content)
        this
    }

    def writeLine(line: Any*): FileWriter = {
        writer.append(line.mkString(this.delimiter) + System.getProperty("line.separator"))
        this
    }

    def writeObjectLine(line: AnyRef): FileWriter = {
        writer.append(Json.serialize(line))
        writer.append(System.getProperty("line.separator"))
        this
    }

    def writeLines(lines: ArrayBuffer[String]): FileWriter = {
        for (line <- lines) {
            writer.append(line + System.getProperty("line.separator"))
        }
        this
    }

    def close(): Unit = {
        writer.close()
    }
}
