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

class FileReader(val file: File, val format: Int, val delimiter: String) {

    def this(file: File) {
        this(file, TextFile.TXT, ",")
    }

    def this(filePath: String) {
        this(new File(filePath.locate()), TextFile.TXT, ",")
    }

    def this(file: File, format: Int) {
        this(file, format, ",")
    }

    def this(filePath: String, format: Int) {
        this(new File(filePath.locate()), format, ",")
    }

    def this(file: File, delimiter: String) {
        this(file, TextFile.TXT, delimiter)
    }

    def this(filePath: String, delimiter: String) {
        this(new File(filePath.locate()), TextFile.TXT, delimiter)
    }

    def this(filePath: String, format: Int, delimiter: String) {
        this(new File(filePath.locate()), format, delimiter)
    }


    //field, defaultValue
    private val fields = new mutable.LinkedHashMap[String, String]()

    if (!file.exists) throw new IOException("File not found: " + file.getPath)

    private val extension = file.getPath.takeAfterLast(".")
    
    private val scanner: Scanner =
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

    def hasNextLine: Boolean = scanner.hasNextLine
        
    def readLine(): String = scanner.nextLine

    def countLines: Int = {
        var lines = 0
        while(this.hasNextLine) {
            this.readLine()
            lines += 1
        }
        this.close()
        lines
    }

    def parseLine: DataRow = {
        if (format != TextFile.JSON) {
            val row = new DataRow()
            val line = this.readLine().split(this.delimiter, -1)
            var i = 0
            for (field <- fields.keys) {
                row.set(field, if (i < line.length) line(i) else fields(field))
                i += 1
            }
            row
        }
        else {
            val row = Json.fromText(this.readLine()).parseRow("/")
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
        lines.mkString(TextFile.TERMINATOR)
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
        lines.mkString(TextFile.TERMINATOR)
    }

    def readAllAsTable(): DataTable = {
        val table = new DataTable()
        while (this.hasNextLine) {
            table.addRow(this.parseLine)
        }
        table
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
        
    def close(): Unit = {
        scanner.close()
    }
}
    

