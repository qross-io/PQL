package cn.qross.fql

import cn.qross.core.{DataCell, DataRow}
import cn.qross.ext.TypeExt._

import scala.collection.mutable

class SET(val expression: String) {


    //分隔成 MAP

    val chars = new mutable.ListBuffer[String]()
    val pieces = new mutable.ArrayBuffer[SetPiece]()

    //提取字符串
    private var exp = expression.pickChars(chars)
    //提取函数

}

class SetPiece(set: SET, val key: String, value: String) {
    def compute(row: DataRow): DataCell = {
        null
    }
}
