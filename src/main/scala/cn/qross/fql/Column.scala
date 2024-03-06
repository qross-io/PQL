package cn.qross.fql

import cn.qross.core.DataType

object Column {
    val CONSTANT: Int = 0
    val MAP: Int = 1
    val FUNCTION: Int = 2
}

class Column(val label: String, val origin: String, val columnType: Int, val dataType: DataType, val value: Any) {

}