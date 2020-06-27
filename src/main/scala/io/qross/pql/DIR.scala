package io.qross.pql

import io.qross.core.{DataCell, DataTable, DataType}
import io.qross.exception.SQLParseException
import io.qross.ext.TypeExt._
import io.qross.fs.Directory
import io.qross.fs.Path._
import io.qross.pql.Patterns.{$DIR, ARROW, $BLANK}
import io.qross.pql.Solver._

object DIR {
    def parse(sentence: String, PQL: PQL): Unit = {
        $DIR.findFirstIn(sentence) match {
            case Some(_) => PQL.PARSING.head.addStatement(new Statement("DIR", sentence, new DIR(sentence)))
            case None => throw new SQLParseException("Wrong DIR sentence: " + sentence)
        }
    }
}

class DIR(sentence: String) {

    def evaluate(PQL: PQL): DataCell = {

        val (dir, links) = {
            if (sentence.contains(ARROW)) {
                (sentence.takeBefore(ARROW), sentence.takeAfter(ARROW))
            }
            else {
                (sentence, "")
            }
        }

        val plan = Syntax("DIR").plan(dir.takeAfter($BLANK).trim().$restore(PQL))

        val path = plan .headArgs
        val data = plan.head match {
            case "LIST" =>
                // 只列表文件夹
                val table = new DataTable()
                Directory.listDirs(path).foreach(file => {
                    table.addRow(file.info)
                })
                DataCell(table, DataType.TABLE)
            case "DELETE" => DataCell(Directory.delete(path), DataType.BOOLEAN)
            case "MOVE" => DataCell.NULL
            case "COPY" => DataCell.NULL
            case "MAKE" => DataCell(Directory.create(path), DataType.BOOLEAN)
            case "SPACE" | "LENGTH" => DataCell(Directory.spaceUsage(path), DataType.INTEGER)
            case "SIZE" | "CAPACITY" => DataCell(Directory.spaceUsage(path).toHumanized, DataType.TEXT)
            case _ =>
                // 列表文件夹和文件
                val table = new DataTable()
                Directory.list(path).foreach(file => {
                    table.addRow(file.info)
                })
                DataCell(table, DataType.TABLE)
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
