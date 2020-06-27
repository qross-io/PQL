package io.qross.pql

import io.qross.core.{DataCell, DataTable, DataType}
import io.qross.exception.SQLParseException
import io.qross.fs.Directory
import io.qross.pql.Patterns.{$BLANK, $FILE, ARROW}
import io.qross.ext.TypeExt._
import io.qross.fs.Path._
import io.qross.pql.Solver._

object REDIS {
    def parse(sentence: String, PQL: PQL): Unit = {
        $FILE.findFirstIn(sentence) match {
            case Some(_) => PQL.PARSING.head.addStatement(new Statement("FILE", sentence, new FILE(sentence)))
            case None => throw new SQLParseException("Wrong FILE sentence: " + sentence)
        }
    }
}

class REDIS(val sentence: String) {

    def evaluate(PQL: PQL): DataCell = {

        val (file, links) = {
            if (sentence.contains(ARROW)) {
                (sentence.takeBefore(ARROW), sentence.takeAfter(ARROW))
            }
            else {
                (sentence, "")
            }
        }

        val plan = Syntax("FILE").plan(file.takeAfter($BLANK).trim().$restore(PQL))

        val path = plan .headArgs
        val data = plan.head match {
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

        if (links != "") {
            new Sharp(links, data).execute(PQL)
        }
        else {
            data
        }
    }

    def execute(PQL: PQL): Unit = {
        val data = this.evaluate(PQL)

        data.dataType match {
            case DataType.TABLE => PQL.RESULT += data.value
                if (PQL.dh.debugging) {
                    data.asTable.show()
                }
            case DataType.ROW => PQL.RESULT += data.value
            case _ =>
        }
    }
}
