package io.qross.fs

import java.io._

import io.qross.core.{DataRow, DataTable}
import io.qross.ext.TypeExt._
import io.qross.fs.Path._
import io.qross.net.Json
import io.qross.setting.Global
import org.springframework.web.context.request.{RequestContextHolder, ServletRequestAttributes}

import scala.collection.mutable.ArrayBuffer

class FileWriter(val filePath: String, val format: Int, val outputType: String, deleteIfExists: Boolean) {

    private var delimiter = "," //default delimiter when you write a collection
    private val file = new File(filePath.locate())
    private var append = false

    def this(filePath: String) {
        this(filePath, TextFile.TXT, TextFile.FILE, false)
    }

    def this(filePath: String, format: Int) {
        this(filePath, format, TextFile.FILE, false)
    }

    def this(filePath: String, outputType: String) {
        this(filePath, TextFile.TXT, outputType, false)
    }

    def this(filePath: String, deleteIfExists: Boolean) {
        this(filePath, TextFile.TXT, TextFile.FILE, deleteIfExists)
    }

    def this(filePath: String, format: Int, outputType: String) {
        this(filePath, format, outputType, false)
    }

    def this(filePath: String, format: Int, deleteIfExists: Boolean) {
        this(filePath, format, TextFile.FILE, deleteIfExists)
    }

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
                response.setCharacterEncoding("UTF-8")
                response.setHeader("Content-disposition", "attachment;filename=\"" + new String(filePath.takeAfterLast("/").getBytes("UTF-8"), "ISO-8859-1") + "\"")
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

        if (filePath.toLowerCase().endsWith(".csv")) {
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

    def writeLine(line: Any*): FileWriter = {
        writer.append(line.mkString(this.delimiter) + System.getProperty("line.separator"))
        this
    }

    def writeObjectLine[T](line: AnyRef): FileWriter = {
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
