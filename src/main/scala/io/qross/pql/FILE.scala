package io.qross.pql

import io.qross.core.{DataCell, DataTable, DataType}
import io.qross.exception.SQLParseException
import io.qross.ext.TypeExt._
import io.qross.fs.Directory
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
            val plan = Syntax("FILE").plan(command)
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
                case _ => println(data.asText)
            }
        }
    }
}
