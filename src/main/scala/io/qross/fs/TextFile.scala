package io.qross.fs

import io.qross.core.{DataHub, DataTable}

object TextFile {
    implicit class DataHub$TextFile(val dh: DataHub) {
        //文本文件相关

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

            def saveAsNewTextFile(fileNameOrFullPath: String, delimiter: String = ","): DataHub = {
                val path = fileNameOrFullPath.locate()
                FileWriter(path, true).delimit(delimiter).writeTable(TABLE).close()
                ZIP.addFile(path)
                this
            }

            def saveAsTextFile(fileNameOrFullPath: String, delimiter: String = ","): DataHub = {
                val path = fileNameOrFullPath.locate()
                FileWriter(path, false).delimit(delimiter).writeTable(TABLE).close()
                ZIP.addFile(path)
                this
            }

            def saveAsNewCsvFile(fileNameOrFullPath: String): DataHub = {
                val path = fileNameOrFullPath.locate()
                FileWriter(path, true).delimit(",").writeTable(TABLE).close()
                ZIP.addFile(path)
                this
            }

            def saveAsCsvFile(fileNameOrFullPath: String): DataHub = {
                val path = fileNameOrFullPath.locate()
                FileWriter(path, false).delimit(",").writeTable(TABLE).close()
                ZIP.addFile(path)
                this
            }

        */
    }
}

class TextFile {

}
