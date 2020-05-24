package io.qross.fql

import io.qross.core.DataTable
import io.qross.pql.Syntax
import io.qross.ext.TypeExt._

class SELECT(sentence: String) {

    //val subqueries =


}

//以后的版本中把解析从Syntax中移出来, 专门搞查询引擎

//执行计划
//IN (SELECT...)
//FROM (SELECT...) A
//JOIN (SELECT...) A
//EXISTS (SELECT...)

//FROM支持的类型
//FROM `mysql.qross`:database.table
//FROM $table
//FROM @buffer
//FROM :virtual