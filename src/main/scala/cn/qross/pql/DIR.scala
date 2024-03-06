package cn.qross.pql

import cn.qross.core.{DataCell, DataTable, DataType}
import cn.qross.ext.TypeExt._
import cn.qross.fs.Directory
import cn.qross.fs.Path._
import cn.qross.pql.Solver._

object DIR {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("DIR", sentence, new DIR(sentence)))
    }
}

class DIR(sentence: String) {

    def evaluate(PQL: PQL, express: Int = Solver.FULL): DataCell = {
        sentence.$process(PQL, express, command => {
            val plan = Syntax("DIR").plan(command.drop(3).trim())
            val path = plan.headArgs
            plan.head match {
                case "LIST" =>
                    // 只列表文件夹
                    val table = new DataTable()
                    Directory.listDirs(path).foreach(file => {
                        table.addRow(file.info)
                    })
                    DataCell(table, DataType.TABLE)
                case "DELETE" => DataCell(Directory.delete(path), DataType.BOOLEAN)
                case "RENAME" => DataCell(path.renameTo(plan.oneArgs("TO")), DataType.BOOLEAN)
                case "MOVE" => DataCell(path.moveTo(plan.oneArgs("TO"), plan.contains("REPLACE EXISTING")))
                case "COPY" => DataCell(path.copyTo(plan.oneArgs("TO"), plan.contains("REPLACE EXISTING")))
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
