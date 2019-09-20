package io.qross.fs

import io.qross.core.{DataHub, SlotObjectNotFoundException}
import io.qross.ext.TypeExt._
import io.qross.net.Json

object TextFile {
    implicit class DataHub$TextFile(val dh: DataHub) {

        def WITH_HEADER: Boolean = {
            if (!dh.slots("WITH_HEADER")) {
                dh.plug("WITH_HEADER", false)
            }
            dh.pick("WITH_HEADER").asInstanceOf[Boolean]
        }

        //private var READER: FileReader = _

        /*
        def openTextFile(path: String): DataHub = {
            READER = FileReader(path)
            this
        }

        def useDelimiter(delimiter: String): DataHub = {
            READER.delimit(delimiter)
            this
        }

        def asTable(tableName: String): DataHub = {
            READER.asTable(tableName)
            this
        }

        def withColumns(fields: String*): DataHub = {
            READER.withColumns(fields: _*)
            this
        }

        def etl(handler: DataTable => DataTable): DataHub = {
            READER.etl(handler)
            this
        }

        /*
        openTextFile("")
            .asTable("")
            .withColumns("")
            .etl(handler)
            .page("SELECT * FROM tableName LIMIT @offset, 10000")

            .get("")
            .page("")
            .block()
        .saveAs("")
            .put("")
         */

        def readAllAsTable(fields: String*): DataHub = {
            TABLE.cut(READER.readAllAsTable(fields: _*))
            this
        }

        */

        def saveAsNewJsonFile(fileNameOrFullPath: String): DataHub = {
            dh.plug("WRITER", FileWriter(fileNameOrFullPath))
                .plug("WRITER_FORMAT", "JSON_LINE")
        }

        def saveAsJsonFile(fileNameOrFullPath: String): DataHub = {
            dh.plug("WRITER", FileWriter(fileNameOrFullPath, deleteFileIfExists = false))
                .plug("WRITER_FORMAT", "JSON_LINE")
        }

        def saveAsNewTextFile(fileNameOrFullPath: String, delimiter: String = ","): DataHub = {
            dh.plug("WRITER", FileWriter(fileNameOrFullPath).delimit(delimiter))
        }

        def saveAsTextFile(fileNameOrFullPath: String, delimiter: String = ","): DataHub = {
            dh.plug("WRITER", FileWriter(fileNameOrFullPath, deleteFileIfExists = false).delimit(delimiter))
        }

        def saveAsNewCsvFile(fileNameOrFullPath: String): DataHub = {
            dh.plug("WRITER", FileWriter(fileNameOrFullPath).delimit(","))
        }

        def saveAsCsvFile(fileNameOrFullPath: String): DataHub = {
            dh.plug("WRITER", FileWriter(fileNameOrFullPath, deleteFileIfExists = false).delimit(","))
        }

        def withHeader(): DataHub = {
            dh.plug("WITH_HEADER", true)
        }

        def withHeader(labels: String): DataHub = {
            dh.plug("WITH_HEADER", true)
            if (labels.bracketsWith("{", "}")) {
                dh.label(
                    Json(labels)
                        .parseRow("/")
                        .toSeq
                        .map(alias => (alias._1, alias._2.asInstanceOf[String])): _*)
            }
            dh
        }

        def withHeader(labels: (String, String)*): DataHub = {
            dh.plug("WITH_HEADER", true)
              .label(labels: _*)
        }

        def write(): DataHub = {
            if (dh.slots("WRITER")) {
                val writer = dh.pick("WRITER").asInstanceOf[FileWriter]
                val format = {
                    if (dh.slots("WRITER_FORMAT")) {
                        dh.pick("WRITER_FORMAT").asInstanceOf[String]
                    }
                    else {
                        "STRING_LINE"
                    }
                }
                if (format == "JSON_LINE") {
                    writer.writeTableAsJsonLine(dh.getData)
                }
                else {
                    writer.writeTable(dh.getData, WITH_HEADER)
                }

                writer.close()
                dh.pull("WRITER")
                dh.pull("WRITER_FORMAT")

                if (dh.slots("ZIP")) {
                    dh.pick("ZIP").asInstanceOf[Zip].addFile(writer.filePath)
                }
            }
            else {
                throw new SlotObjectNotFoundException("Must use SAVE AS method to save file first.")
            }
            dh
        }
    }
}