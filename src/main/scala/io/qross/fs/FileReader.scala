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
import io.qross.fs.FilePath._
import io.qross.net.Json

//在hive相关的包中, 将来移到HDFSReader中
//import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream

import scala.collection.mutable

object FileReader {
    val DATA: ConcurrentLinkedQueue[DataTable] = new ConcurrentLinkedQueue[DataTable]()
}

case class FileReader(filePath: String) {

    private val CHARSET = "utf-8"
    private var delimiter: String = ","
    //field, defaultValue
    private val fields = new mutable.LinkedHashMap[String, String]()
    
    private val file = new File(filePath.locate())
    if (!file.exists) throw new IOException("File not found: " + filePath)
    private val extension = filePath.substring(filePath.lastIndexOf("."))
    
    private val scanner: Scanner =
        if (".log".equalsIgnoreCase(extension) || ".txt".equalsIgnoreCase(extension) || ".csv".equalsIgnoreCase(extension)) {
            new Scanner(this.file, CHARSET)
        }
        else if (".gz".equalsIgnoreCase(extension)) {
            new Scanner(new GZIPInputStream(new FileInputStream(this.file)), CHARSET)
        }
//        else if (".bz2".equalsIgnoreCase(extension)) {
//            new Scanner(new BZip2CompressorInputStream(new FileInputStream(this.file)), CHARSET)
//        }
        else {
            throw new IOException("Unrecognized Format: " + this.filePath)
        }

    def delimit(delimiter: String): FileReader = {
        this.delimiter = delimiter
        this
    }

    def jsonObjectLine: FileReader = {
        this.delimiter = "JSON$OBJECT$LINE"
        this
    }

    //field, defaultValue
    def withDefaultValues(fields: (String, String)*): FileReader = {
        this.fields ++= fields
        this
    }

    def withColumns(fields: String*): FileReader = {
        fields.foreach(field => this.fields += field -> null)
        this
    }

    def etl(handler: DataTable => Unit): FileReader = {

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
                    Timer.sleep(0.1F)
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
    /*
    def readAsTable(fields: String*): DataTable = {
        val table = new DataTable()
        while (this.hasNextLine) {
            val line = this.readLine.split(this.delimiter, -1)
            val row = DataRow()
            for (i <- 0 until fields.length) {
                if (i < line.length) {
                    row.set(fields(i), line(i))
                }
            }
            table.addRow(row)
        }
        table
    }*/

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
    

