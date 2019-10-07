package io.qross.pql

import io.qross.core.{DataCell, DataRow}

import scala.collection.mutable.ArrayBuffer

class Statement(val caption: String, val sentence: String = "", val instance: AnyRef = null) {

    //所有子语句
    lazy private[pql] val statements = new ArrayBuffer[Statement]()

    //局部变量列表，对于root，则表示全局变量
    lazy private[pql] val variables = new DataRow()

    def containsVariable(name: String): Boolean = this.variables.contains(name)
    def getVariable(name: String): DataCell = this.variables.getCell(name)
    def setVariable(name: String, value: Any): Unit = {
        value match {
            case cell: DataCell => this.variables.set(name, cell)
            case _ => this.variables.set(name, value)
        }
    }

    def addStatement(statement: Statement): Unit = this.statements += statement
}