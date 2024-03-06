package cn.qross.pql

import cn.qross.core.{DataCell, DataHub, DataTable, DataType}
import cn.qross.exception.SQLParseException
import cn.qross.ext.TypeExt._
import cn.qross.fs.Path._
import cn.qross.fs.{Directory, FileReader, FileWriter}
import cn.qross.pql.Solver._

object FILE {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("FILE", sentence, new FILE(sentence)))
    }
}

class FILE(val sentence: String) {
    
    def evaluate(PQL: PQL, express: Int = Solver.FULL): DataCell = {
        sentence.$process(PQL, express, command => {
            val plan = Syntax("FILE").plan(command.drop(4).trim())
            val path = plan .headArgs
            plan.head match {
                case "LIST" =>
                    val table = new DataTable();
                    {
                        if (path.isDir) {
                            Directory.listFiles(path)
                        }
                        else {
                            val dir = path.locate()
                            Directory.listFiles(dir.takeBeforeLast("/"), dir.takeAfterLast("/"))
                        }
                    }.foreach(file => {
                        table.addRow(file.getAbsolutePath.fileInfo)
                    })
                    DataCell(table, DataType.TABLE)
                case "DELETE" => DataCell(path.delete(), DataType.BOOLEAN)
                case "RENAME" => DataCell(path.renameTo(plan.oneArgs("TO")), DataType.BOOLEAN)
                case "MOVE" => DataCell(path.moveTo(plan.oneArgs("TO"), plan.contains("REPLACE EXISTING")))
                case "COPY" => DataCell(path.copyTo(plan.oneArgs("TO"), plan.contains("REPLACE EXISTING")))
                case "MAKE" => DataCell(path.makeFile(), DataType.BOOLEAN)
                case "LENGTH" => DataCell(path.fileLength(), DataType.INTEGER)
                case "SIZE" => DataCell(path.fileLength().toHumanized, DataType.TEXT)
                case "READ" => DataCell(new FileReader(path).readToEnd, DataType.TEXT)
                case "WRITE" =>
                    // WRITE content TO FILE file;
                    // FILE WRITE content TO file;
                    if (plan.contains("APPEND")) {
                        try {
                            new FileWriter(path).write(plan.oneArgs("APPEND")).close()
                            DataCell(true, DataType.BOOLEAN)
                        }
                        catch {
                            case e: Exception =>
                                e.printStackTrace()
                                DataCell(e.getMessage, DataType.TEXT)
                        }
                    }
                    else {
                        throw new SQLParseException("Miss phrase: APPEND 'filePath'. " + sentence)
                    }
                case "VOYAGE" =>
                    val pql = new PQL(new FileReader(path).readToEnd, embedded = true, DataHub.DEFAULT)
                    if (plan.contains("WITH")) {
                        pql.place(plan.mapArgs("WITH"))
                    }
                    if (plan.contains("TO")) {
                        new FileWriter(plan.oneArgs("TO"), true).write(pql.run().asInstanceOf[String]).close()
                    }
                    DataCell(plan.oneArgs("TO"), DataType.TEXT)
                case "DOWNLOAD" => DataCell(path.download()) //, DataType.BOOLEAN
                case _ => path.fileInfo.toDataCell(DataType.ROW)
            }
        })
    }
    
    def execute(PQL: PQL): Unit = {
        val data = this.evaluate(PQL)
        PQL.WORKING += data.value

        if (PQL.dh.debugging) {
            data.dataType match {
                case DataType.TABLE => data.asTable.show()
                case _ => println(sentence.$restore(PQL).take(100) + ": " + data.asText)
            }
        }
    }
}
