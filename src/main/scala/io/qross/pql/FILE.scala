package io.qross.pql

import io.qross.core.{DataCell, DataTable, DataType}
import io.qross.exception.SQLParseException
import io.qross.ext.TypeExt._
import io.qross.fs.{Directory, FileReader, FileWriter}
import io.qross.fs.Path._
import io.qross.pql.Patterns.{$BLANK, $FILE, ARROW}
import io.qross.pql.Solver._

object FILE {
    def parse(sentence: String, PQL: PQL): Unit = {
        $FILE.findFirstIn(sentence) match {
            case Some(_) => PQL.PARSING.head.addStatement(new Statement("FILE", sentence, new FILE(sentence)))
            case None => throw new SQLParseException("Wrong FILE sentence: " + sentence)
        }
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
                case "MOVE" => DataCell.NULL
                case "COPY" => DataCell.NULL
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
                case _ => println(sentence + ": " + data.asText)
            }
        }
    }
}
