package io.qross.pql

import scala.collection.mutable.ArrayBuffer

//函数
/*

FUNCTION $/@func_name ($a INT DEFAULT 0, $b TEXT DEFAULT 'hello')
    BEGIN
        RETURN $b + $a;
    END;

 */

class UserFunction(statement: Statement) {

    val variables = new ArrayBuffer[Int]()

    val statements = new ArrayBuffer[Statement]()
    statements ++= statement.statements


}
