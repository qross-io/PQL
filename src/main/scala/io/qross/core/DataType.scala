package io.qross.core

import com.fasterxml.jackson.databind.JsonNode
import io.qross.ext.Output
import io.qross.pql.SQLParseException

//case class DataType(className: String, typeName: String)

object DataType extends Enumeration {
    val INTEGER: DataType = DataType("INTEGER")
    val DECIMAL: DataType = DataType("DECIMAL")
    val TEXT: DataType = DataType("TEXT")
    val BLOB: DataType = DataType("BLOB")
    val NULL: DataType = DataType("NULL")
    val DATETIME: DataType = DataType("DATETIME")
    val BOOLEAN: DataType = DataType("BOOLEAN")
    val TABLE: DataType = DataType("TABLE")
    val ROW: DataType = DataType("ROW")
    val MAP: DataType = DataType("MAP")
    val LIST: DataType = DataType("LIST")
    val ARRAY: DataType = DataType("ARRAY")
    val OBJECT: DataType = DataType("OBJECT")
    val JSON: DataType = DataType("JSON")
    val EXCEPTION: DataType = DataType("EXCEPTION")
    val REGEX: DataType = DataType("REGEX")

    //database data type name
    def ofTypeName(typeName: String, className: String = ""): DataType = {
        {
            typeName.toUpperCase match {
                case "TEXT" | "VARCHAR" | "CHAR" | "NVARCHAR" | "TINYTEXT" | "SMALLTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "STRING" | "NVARCHAR" | "NCHAR" => DataType.TEXT
                case "INT" | "INTEGER" | "BIGINT" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "LONG" | "SHORT" => DataType.INTEGER
                case "FLOAT" | "DOUBLE" | "DECIMAL" | "REAL" | "NUMBER" | "NUMERIC" | "MONEY" | "SMALLMONEY" => DataType.DECIMAL
                case "DATE" | "TIME" | "DATETIME" | "TIMESTAMP" | "YEAR" => DataType.DATETIME
                case "BIT" | "BOOL" | "BOOLEAN" => DataType.BOOLEAN
                case "MAP" | "ROW" | "OBJECT" | "DATAROW" => DataType.ROW
                case "TABLE" | "DATATABLE" => DataType.TABLE
                case "NULL" | "EMPTY" => DataType.NULL
                case "JSON" => DataType.JSON
                case "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "VARBINARY" | "BINARY" => DataType.BLOB
                case _ =>
                    if (className != "") {
                        ofClassName(className)
                    }
                    else {
                        DataType.NULL
                    }
            }
        }.of(typeName.toUpperCase())
            .from(className)
    }

    //java type name
    def ofClassName(className: String): DataType = {
        val name = {
            if (className.contains(".")) {
                className.substring(className.lastIndexOf(".") + 1).toLowerCase()
            }
            else {
                className.toLowerCase()
            }
        }

        {
            name match {
                case "string" => DataType.TEXT
                case "bit" | "int" | "integer" | "long" | "timestamp" => DataType.INTEGER
                case "float" | "double" | "bigdecimal" => DataType.DECIMAL
                case "boolean" => DataType.BOOLEAN
                case "datetime" | "date" | "time" | "timestamp" => DataType.DATETIME
                case "list" | "array" | "arraylist" => DataType.ARRAY
                case "map" => DataType.MAP
                case "datarow" => DataType.ROW
                case "datatable" => DataType.TABLE
                case "regex" | "pattern" => DataType.REGEX
                case "json" => DataType.JSON
                case "[B" => DataType.BLOB
                case _ => DataType.TEXT
            }
        }.from(className)
    }

    //data value
    def ofValue(value: Any): DataType = {
        if (value != null) {
            ofClassName(value.getClass.getName)
        }
        else {
            DataType.NULL
        }
    }

    def ofJsonNodeType(node: JsonNode): DataType = {
        {
            if (node.isIntegralNumber || node.isInt || node.isLong || node.isShort || node.isBigInteger) {
                DataType.INTEGER
            }
            else if (node.isFloatingPointNumber || node.isDouble || node.isFloat || node.isBigDecimal) {
                DataType.DECIMAL
            }
            else if (node.isBoolean) {
                DataType.BOOLEAN
            }
            else if (node.isArray) {
                DataType.ARRAY
            }
            else if (node.isBinary) {
                DataType.BLOB
            }
            else if (node.isNull) {
                DataType.NULL
            }
            else if (node.isObject) {
                DataType.ROW
            }
            else {
                DataType.TEXT
            }
        }.from(node.getNodeType.name())
    }
}

case class DataType(typeName: String) {
    //来自数据库的原始类型, 如INT, VARCHAR等
    var originalName: String = ""
    //语言类类型, 如java.lang.String
    var className: String = ""

    def of(originalName: String): DataType = {
        this.originalName = originalName
        this
    }

    def from(className: String): DataType = {
        this.className = className
        this
    }

    override def toString: String = typeName
}

/*
-- DataType --


-- JsonNodeDataType --
ARRAY,
BINARY,
BOOLEAN,
MISSING,
NULL,
NUMBER,
OBJECT,
POJO,
STRING

-- SQLite DataTypes --
NULL
INTEGER
REAL
TEXT
BLOB
*/

/*

CREATE TABLE data_types (
    id BIGINT DEFAULT 1,
    t1 TINYINT DEFAULT 1,
    t2 SMALLINT DEFAULT 1,
    t3 MEDIUMINT DEFAULT 1,
    t4 INT DEFAULT 1,
    t5 BIGINT DEFAULT 1,
    t6 FLOAT DEFAULT 1.1,
    t7 DOUBLE DEFAULT 1.1,
    t8 DECIMAL(15,4) DEFAULT 1.1,
    t9 DATE,
    ta TIME,
    tb YEAR,
    tc DATETIME,
    td TIMESTAMP,
    te CHAR,
    tf VARCHAR(10),
    tg TINYBLOB,
    th TINYTEXT,
    ti BLOB,
    tj TEXT,
    tk MEDIUMBLOB,
    tl MEDIUMTEXT,
    tm LONGBLOB,
    tn LONGTEXT
);

t1, java.lang.Integer, TINYINT-6
t2, java.lang.Integer, SMALLINT5
t3, java.lang.Integer, MEDIUMINT4
t4, java.lang.Integer, INT4
t5, java.lang.Long, BIGINT-5
t6, java.lang.Float, FLOAT7
t7, java.lang.Double, DOUBLE8
t8, java.math.BigDecimal, DECIMAL3
t9, java.sql.Date, DATE91
ta, java.sql.Time, TIME92
tb, java.sql.Date, YEAR91
tc, java.sql.Timestamp, DATETIME93
td, java.sql.Timestamp, TIMESTAMP93
te, java.lang.String, CHAR1
tf, java.lang.String, VARCHAR12
tg, [B, TINYBLOB-3
th, java.lang.String, VARCHAR-1
ti, [B, BLOB-4
tj, java.lang.String, VARCHAR-1
tk, [B, MEDIUMBLOB-4
tl, java.lang.String, VARCHAR-1
tm, [B, LONGBLOB-4
tn, java.lang.String, VARCHAR-1

-- MySQL DataTypes --
TINYINT
SMALLINT
MEDIUMINT
INT
BIGINT
FLOAT
DOUBLE
DECIMAL

DATE
TIME
YEAR
DATETIME
TIMESTAMP

CHAR
VARCHAR
TINYBLOB
TINYTEXT
BLOB
TEXT
MEDIUMBLOB
MEDIUMTEXT
LONGBLOB
LONGTEXT
*/