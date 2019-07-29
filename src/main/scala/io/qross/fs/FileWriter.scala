package io.qross.fs

import java.io._

import io.qross.fs.FilePath._
import io.qross.core.DataTable

import scala.collection.mutable.ArrayBuffer

case class FileWriter(filePath: String, deleteFileIfExists: Boolean = true) {

    private var delimiter = "," //default delimiter when you write a collection
    private val file = new File(filePath.locate())

    if (file.exists() && deleteFileIfExists) {
        file.delete()
    }
    if (!file.exists()) {
        file.getParentFile.mkdirs()
        file.createNewFile()
    }

    //append = true
    private val fos = new FileOutputStream(file, true)
    if (filePath.toLowerCase().endsWith(".csv")) {
        fos.write(Array[Byte](0xEF.toByte, 0xBB.toByte, 0xBF.toByte))
    }
    private val output: BufferedWriter = new BufferedWriter(new OutputStreamWriter(fos, "utf-8"))
    //PrintWriter and FileWriter is advanced implements of OutputStreamWriter
    //private val output = new PrintWriter(file)
    //private val output = new BufferedWriter(new java.io.FileWriter(file))

    def delimit(delimiter: String): FileWriter = {
        this.delimiter = delimiter
        this
    }

    def writeTable(table: DataTable): FileWriter = {
        writeLine(table.getLabelNames.mkString(delimiter))
        table.foreach(row => {
            writeLine(row.getValues.mkString(delimiter))
        })
        this
    }

    def writeLine(line: Any*): FileWriter = {
        output.append(line.mkString(this.delimiter) + System.getProperty("line.separator"))
        this
    }

    def writeLines(lines: ArrayBuffer[String]): FileWriter = {
        for (line <- lines) {
            output.append(line + System.getProperty("line.separator"))
        }
        this
    }

    def close(): Unit = {
        output.close()
    }
}
