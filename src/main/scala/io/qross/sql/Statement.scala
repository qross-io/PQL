package io.qross.sql

import io.qross.core.{DataCell, DataRow}
import io.qross.ext.Output

import scala.collection.mutable.ArrayBuffer

class Statement(val caption: String, val sentence: String = "", val instance: AnyRef = null) {

    //表示控制语句是否闭合, 用于解析检查
    //var closed: Boolean = true
    //所有子语句
    val statements = new ArrayBuffer[Statement]()

    //局部变量列表，对于root，则表示全局变量
    private val variables = new DataRow()

    def containsVariable(name: String): Boolean = this.variables.contains(name)

    def getVariable(name: String): DataCell = this.variables.getCell(name)

    def setVariable(name: String, value: Any): Unit = {
        value match {
            case cell: DataCell => this.variables.set(name, cell)
            case _ => this.variables.set(name, value)
        }
    }

    def addStatement(statement: Statement): Unit = this.statements += statement

    def show(level: Int): Unit = {
        for (i <- 0 until level) {
            System.out.print("\t")
        }
        Output.writeLine(this.sentence)
        for (statement <- this.statements) {
            statement.show(level + 1)
        }
    }
}