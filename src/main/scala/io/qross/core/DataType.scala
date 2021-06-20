package io.qross.core

import com.fasterxml.jackson.databind.JsonNode
import io.qross.ext.TypeExt._

object DataType extends Enumeration {
    val INTEGER: DataType = new DataType("INTEGER")
    val DECIMAL: DataType = new DataType("DECIMAL")
    val TEXT: DataType = new DataType("TEXT")
    val BLOB: DataType = new DataType("BLOB")
    val NULL: DataType = new DataType("NULL")
    val DATETIME: DataType = new DataType("DATETIME")
    val BOOLEAN: DataType = new DataType("BOOLEAN")
    val TABLE: DataType = new DataType("TABLE")
    val ROW: DataType = new DataType("ROW")
    val LIST: DataType = new DataType("LIST")
    val ARRAY: DataType = new DataType("ARRAY")
    val HASHSET: DataType = new DataType("HASHSET")
    val JSON: DataType = new DataType("JSON")
    val EXCEPTION: DataType = new DataType("EXCEPTION")
    val REGEX: DataType = new DataType("REGEX")

    def isInternalType(dataType: DataType): Boolean = {
        "TEXT,INTEGER,DECIMAL,BOOLEAN,DATETIME,ARRAY,LIST,ROW,TABLE,JSON,EXCEPTION,REGEX,NULL".contains(dataType.typeName)
    }

    //database data type name
    def ofTypeName(typeName: String, className: String = ""): DataType = {
        if (typeName != null) {
            new DataType(
                typeName.takeBeforeIfContains(" ").toUpperCase match {
                    case "TEXT" | "VARCHAR" | "CHAR" | "NVARCHAR" | "TINYTEXT" | "SMALLTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "STRING" | "NVARCHAR" | "NCHAR" => "TEXT"
                    case "INT" | "INTEGER" | "BIGINT" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "LONG" | "SHORT" => "INTEGER"
                    case "FLOAT" | "DOUBLE" | "DECIMAL" | "REAL" | "NUMBER" | "NUMERIC" | "MONEY" | "SMALLMONEY" => "DECIMAL"
                    case "DATE" | "TIME" | "DATETIME" | "TIMESTAMP" | "YEAR" => "DATETIME"
                    case "BIT" | "BOOL" | "BOOLEAN" => "BOOLEAN"
                    case "MAP" | "ROW" | "OBJECT" | "DATAROW" => "ROW"
                    case "TABLE" | "DATATABLE" => "TABLE"
                    case "NULL" | "EMPTY" => "NULL"
                    case "JSON" => "JSON"
                    case "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "VARBINARY" | "BINARY" => "BLOB"
                    case _ => DataType.ofClassName(className).typeName
                }, typeName.toUpperCase(), className)
        }
        else {
            DataType.ofClassName(className)
        }
    }

    //从java类名判断
    // java class name
    def ofClassName(className: String): DataType = {
        val firstName = {
            if (className != null && className != "") {
                if (className.contains(".")) {
                    className.substring(className.lastIndexOf(".") + 1)
                }
                else {
                    className
                }
            }
            else {
                "TEXT"
            }
        }

        new DataType(
             firstName.toLowerCase() match {
                    case "string" | "object" | "text" => "TEXT"
                    case "bit" | "int" | "integer" | "bigint" | "biginteger" | "long" | "timestamp" => "INTEGER"
                    case "float" | "double" | "bigdecimal" | "decimal" => "DECIMAL"
                    case "boolean" => "BOOLEAN"
                    case "datetime" | "date" | "time" | "timestamp" => "DATETIME"
                    case "list" | "array" | "arraylist" => "ARRAY"
                    case "set" | "hashset" | "treeset" => "HASHSET"
                    case "map" => "ROW"
                    case "datarow" => "ROW"
                    case "datatable" => "TABLE"
                    case "regex" | "pattern" => "REGEX"
                    case "json" => "JSON"
                    case _ => className
                }, firstName.toUpperCase(), className)
    }

    // 从值判断数据类型
    // data value
    def ofValue(value: Any): DataType = {
        if (value != null) {
            DataType.ofClassName(value.getClass.getName)
        }
        else {
            DataType.NULL
        }
    }

    //从json节点判断类型
    def ofJsonNodeType(node: JsonNode): DataType = {

        new DataType(
            if (node.isIntegralNumber || node.isInt || node.isLong || node.isShort || node.isBigInteger) {
                "INTEGER"
            }
            else if (node.isFloatingPointNumber || node.isDouble || node.isFloat || node.isBigDecimal) {
                "DECIMAL"
            }
            else if (node.isBoolean) {
                "BOOLEAN"
            }
            else if (node.isArray) {
                "ARRAY"
            }
            else if (node.isBinary) {
                "BLOB"
            }
            else if (node.isNull) {
                "NULL"
            }
            else if (node.isObject) {
                "ROW"
            }
            else {
                "TEXT"
            }, {
                val name = node.getNodeType.name()
                if (name.contains(".")) {
                    name.takeAfterLast(".")
                }
                else {
                    name
                }
            }.toUpperCase(), node.getNodeType.name())
    }

    def forClassName(className: String): DataType = {
        new DataType(className, (if (className.contains(".")) className.takeAfterLast(".") else className).toUpperCase(), className)
    }
}

// typeName 类型名, 如 TEXT  INTEGER 等
// originalName来自数据库的原始类型, 如INT, VARCHAR等
// className 语言类类型, 如java.lang.String
class DataType(val typeName: String, val originalName: String, val className: String) {

    def this(typeName: String) {
        this(typeName, "", "")
    }

    override def toString: String = typeName

    override def equals(obj: scala.Any): Boolean = {
        obj match {
            case dataType: DataType => this.typeName == dataType.typeName
            case str: String => this.typeName.equalsIgnoreCase(str)
            case _ => false
        }
    }
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