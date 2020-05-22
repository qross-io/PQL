package io.qross.fql

import io.qross.core.DataTable
import io.qross.pql.Syntax
import io.qross.ext.TypeExt._

class SELECT(sentence: String) {



    def executeDataTable(source: Any = null): DataTable = {



        new DataTable()
    }
}

//以后的版本中把解析从Syntax中移出来, 专门搞查询引擎

//执行计划
//IN (SELECT...)
//FROM (SELECT...) A
//JOIN (SELECT...) A
//EXISTS (SELECT...)

//FROM支持的类型
//FROM `mysql.qross`.database.table
//FROM $table
//FROM @buffer
//FROM TXT FILE *.txt SEEK cursor DELIMITED BY 'char'
//FROM CSV FILE *.csv SEEK cursor
//FROM GZ FILE *.gz DELIMITED BY 'char'
//FROM SHEET sheetName
//FROM JSON [{"name": "Tom", age: 18 }, {"name":"Ted", age: 10 }]
//FROM JSON FILE *.json SEEK cursor
//FROM JSON API '/api/test' METHOD 'GET' PATH '/'
//FROM HDFS '/api/test'