package io.qross.pql

import io.qross.core.DataRow

import scala.collection.mutable.ArrayBuffer

//函数
/*

FUNCTION $/@func_name ($a INT DEFAULT 0, $b TEXT DEFAULT 'hello')
    BEGIN
        RETURN $b + $a;
    END;

 */

class UserFunction(statement: Statement) {
    //复制变量
    val variables: DataRow = DataRow.from(statement.variables)
    //复制语句
    val statements: ArrayBuffer[Statement] = new ArrayBuffer[Statement]() ++= statement.statements
}