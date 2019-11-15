package io.qross.fs

import java.io.{File, FileInputStream, IOException}
import java.util.Scanner
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.zip.GZIPInputStream

import io.qross.core.{DataRow, DataTable}
import io.qross.setting.{Environment, Global}
import io.qross.thread.{Cube, Parallel}
import io.qross.time.Timer
import io.qross.ext._
import io.qross.fs.Path._
import io.qross.net.Json
import io.qross.ext.TypeExt._

//在hive相关的包中, 将来移到HDFSReader中
//import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream

import scala.collection.mutable

object FileReader {
    val DATA: ConcurrentLinkedQueue[DataTable] = new ConcurrentLinkedQueue[DataTable]()
}

case class FileReader(file: File) {

    def this(filePath: String) {
        this(new File(filePath.locate()))
    }

    private var delimiter: String = ","
    //field, defaultValue
    private val fields = new mutable.LinkedHashMap[String, String]()

    if (!file.exists) throw new IOException("File not found: " + file.getPath)
    private val extension = file.getPath.takeAfterLast(".")
    
    private val scanner: Scanner =
//        if (".log".equalsIgnoreCase(extension) || ".txt".equalsIgnoreCase(extension) || ".csv".equalsIgnoreCase(extension)) {
//
//        }
        if (".gz".equalsIgnoreCase(extension)) {
            new Scanner(new GZIPInputStream(new FileInputStream(this.file)), Global.CHARSET)
        }
        else {
            new Scanner(this.file, Global.CHARSET)
        }
//        else if (".bz2".equalsIgnoreCase(extension)) {
//            new Scanner(new BZip2CompressorInputStream(new FileInputStream(this.file)), CHARSET)
//        }
//        else {
//            throw new IOException("Unrecognized Format: " + file.getPath)
//        }

    def delimitedBy(delimiter: String): FileReader = {
        this.delimiter = delimiter
        this
    }

    //file is json
    //readLineAsJsonObject
    def asJsonLines: FileReader = {
        this.delimiter = "JSON$OBJECT$LINE"
        this
    }

    //field, defaultValue
    def mapColumnsWithDefaultValues(fields: (String, String)*): FileReader = {
        this.fields ++= fields
        this
    }

    def mapColumns(fields: String*): FileReader = {
        fields.foreach(field => this.fields += field -> null)
        this
    }

    def readAsTable(handler: DataTable => Unit): FileReader = {

        val table = new DataTable()
        while (this.hasNextLine) {
            table.addRow(this.parseLine)
            if (table.count() >= 10000) {
                handler(table)
                table.clear()
            }
        }

        if (table.count() > 0) {
            handler(table)
        }

        this
    }

    def consumeAsTable(handler: DataTable => Unit): FileReader = {

        val cube = new Cube()
        val parallel = new Parallel()
        //consumer
        for (i <- 0 until Environment.cpuThreads * 2) {
            parallel.add(new FileReaderConsumer(cube, handler))
        }
        parallel.startAll()

        //producer
        var table = new DataTable()
        while (this.hasNextLine) {

            table.addRow(this.parseLine)

            if (table.count() >= 10000) {
                FileReader.DATA.add(table)
                table = new DataTable()

                while (FileReader.DATA.size() >= 3) {
                    Timer.sleep(100)
                }
            }
        }

        if (table.count() > 0) {
            FileReader.DATA.add(table)
        }
        cube.reset()

        parallel.waitAll()
        Output.writeMessage("Finish reading.")

        this
    }

    def hasNextLine: Boolean = scanner.hasNextLine
        
    def readLine: String = scanner.nextLine

    def countLines: Int = {
        var lines = 0
        while(this.hasNextLine) {
            this.readLine
            lines += 1
        }
        this.close()
        lines
    }

    def parseLine: DataRow = {
        if (this.delimiter != "JSON$OBJECT$LINE") {
            val row = new DataRow()
            val line = this.readLine.split(this.delimiter, -1)
            var i = 0
            for (field <- fields.keys) {
                row.set(field, if (i < line.length) line(i) else fields(field))
                i += 1
            }
            row
        }
        else {
            val row = Json.fromText(this.readLine).parseRow("/")
            for (field <- fields.keys) {
                if (!row.contains(field)) {
                    row.set(field, fields(field))
                }
            }
            row
        }
    }

    def readToEnd: String = {
        var lines = new mutable.ArrayBuffer[String]()
        while(this.hasNextLine) {
            lines += this.readLine
        }
        this.close()
        lines.mkString("\r\n")
    }

    def readToEnd(filter: String => Boolean): String = {
        var lines = new mutable.ArrayBuffer[String]()
        while(this.hasNextLine) {
            val line = this.readLine
            if (filter(line)) {
                lines += line
            }
        }
        this.close()
        lines.mkString("\r\n")
    }

    //field, defaultValue
    def readAllAsTable(fields: String*): DataTable = {
        val table = new DataTable()
        while (this.hasNextLine) {
            table.addRow(this.parseLine)
        }
        table
    }
        
    def close(): Unit = {
        scanner.close()
    }
}
    

