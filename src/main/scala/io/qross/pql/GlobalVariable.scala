package io.qross.pql

import io.qross.core.{DataCell, DataRow, DataType}
import io.qross.exception.SQLExecuteException
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.net.{Cookies, Session}
import io.qross.pql.Patterns.FUNCTION_NAMES
import io.qross.setting.{Configurations, Language}
import io.qross.time.DateTime
import io.qross.fs.TextFile._

object GlobalVariable {

    //除环境全局变量的其他全局变量
    //系统全局变量, 仅作用于当前PQL
    val SYSTEM: DataRow = new DataRow()
    //用户全局变量
    val USER: DataRow = new DataRow()

    //Global.getClass.getDeclaredMethods.contains()
    //Global.getClass.getMethod("").invoke(null)

    if (JDBC.hasQrossSystem) {
        DataSource.QROSS.queryDataTable("SELECT var_group, var_name, var_type, var_value FROM qross_variables WHERE var_group='SYSTEM'")
                .foreach(row => {
                    SYSTEM.set(
                        row.getString("var_name").toUpperCase()
                        , row.getString("var_type") match {
                            case "INTEGER" => row.getLong("var_value")
                            case "DECIMAL" => row.getDouble("var_value")
                            case _ => row.getString("var_value")
                        })

                }).clear()
    }

    def loadUserVariables(userId: Int): Unit = {
        //从数据库加载用户全局变量
        if (JDBC.hasQrossSystem) {
            DataSource.QROSS.queryDataTable("SELECT var_name, var_type, var_value FROM qross_variables WHERE var_user=?", userId)
                    .foreach(row => {
                        GlobalVariable.USER.set(
                            row.getString("var_name").toUpperCase(),
                            row.getString("var_type") match {
                                case "INTEGER" => row.getLong("var_value")
                                case "DECIMAL" => row.getDouble("var_value")
                                case _ => row.getString("var_value")
                            })

                    }).clear()
        }
    }

    //设置临时系统变量, 系统启动时更新, 关闭时删除, 只作用于当前系统
    //如果设置了与更高级的全局变量相同的变量名, 无意义
    def set(name: String, value: Any): Unit = {
        SYSTEM.set(name, value)
    }

    //设置只作用于当前PQL的全局变量, 暂时无用
//    def set(name: String, value: Any, PQL: PQL): Unit = {
//        PQL.variables.set(name, value)
//    }

    //更新用户变量
    def set(name: String, value: Any, username: String, role: String): Unit = {

        if (Patterns.GLOBAL_VARIABLES.contains(name)) {
            throw new SQLExecuteException("Can't update process variable. This variable is readonly.")
        }
        else if (FUNCTION_NAMES.contains(name)) {
            throw new SQLExecuteException(s"$name is a global function.")
        }
        else if (SYSTEM.contains(name) || Configurations.contains(name)) {
            if (role == "master") {
                if (SYSTEM.contains(name)) {
                    SYSTEM.set(name, value)
                    //更新数据库
                    DataSource.QROSS.queryUpdate(s"""INSERT INTO qross_variables (var_group, var_type, var_user, var_name, var_value) VALUES ('SYSTEM', ?, 0, ?, ?) ON DUPLICATE KEY UPDATE var_value=?""", value match {
                        case _: Int => "INTEGER"
                        case _: Long => "INTEGER"
                        case _: Float => "DECIMAL"
                        case _: Double => "DECIMAL"
                        case _ => "TEXT"
                    }, name, value, value)
                }
                else if (Configurations.contains(name)) {
                    Configurations.set(name, value)
                }
            }
            else {
                throw new SQLExecuteException(s"Can't update system variable. This variable is readonly for $role.")
            }
        }
        else {
            USER.set(name, value)
            //更新数据库
            if (JDBC.hasQrossSystem) {
                DataSource.QROSS.queryUpdate(s"""INSERT INTO qross_variables (var_group, var_type, var_user, var_name, var_value) VALUES ('USER', ?, ?, ?, ?) ON DUPLICATE KEY UPDATE var_value=?""", value match {
                    case _: Int => "INTEGER"
                    case _: Long => "INTEGER"
                    case _: Float => "DECIMAL"
                    case _: Double => "DECIMAL"
                    case _ => "TEXT"
                }, username, name, value, value)
            }
        }
    }

    //得到全局变量的值
    def get(name: String, PQL: PQL ): DataCell = {
        if (Patterns.GLOBAL_VARIABLES.contains(name)) {
            Class.forName("io.qross.pql.GlobalVariableDeclaration")
                .getDeclaredMethod(name, Class.forName("io.qross.pql.PQL"))
                .invoke(null, PQL).asInstanceOf[DataCell]
        }
        else if (USER.contains(name)) {
            USER.getCell(name)
        }
        else if (SYSTEM.contains(name)) {
            SYSTEM.getCell(name)
        }
        else if (PQL.credential.contains(name)) {
            PQL.credential.getCell(name)
        }
        else if (Configurations.contains(name)) {
            DataCell(Configurations.getOrProperty(name.toUpperCase, name.replace("_", ".").toLowerCase(), null))
        }
        else {
            DataCell.UNDEFINED
        }
    }
}

object GlobalVariableDeclaration {
    def TODAY(PQL: PQL): DataCell = DataCell(DateTime.now.setZeroOfDay(), DataType.DATETIME)
    def YESTERDAY(PQL: PQL): DataCell = DataCell(DateTime.now.minusDays(1).setZeroOfDay(), DataType.DATETIME)
    def TOMORROW(PQL: PQL): DataCell = DataCell(DateTime.now.plusDays(1).setZeroOfDay(), DataType.DATETIME)
    def NOW(PQL: PQL): DataCell = DataCell(DateTime.now, DataType.DATETIME)

    def ROWS(PQL: PQL): DataCell = DataCell(PQL.COUNT_OF_LAST_SELECT, DataType.INTEGER)
    def COUNT_OF_LAST_SELECT(PQL: PQL): DataCell = DataCell(PQL.COUNT_OF_LAST_SELECT, DataType.INTEGER)
    def COUNT(PQL: PQL): DataCell = DataCell(PQL.dh.COUNT_OF_LAST_GET, DataType.INTEGER)
    def TOTAL(PQL: PQL): DataCell = DataCell(PQL.dh.TOTAL_COUNT_OF_RECENT_GET, DataType.INTEGER)
    def COUNT_OF_LAST_GET(PQL: PQL): DataCell = DataCell(PQL.dh.COUNT_OF_LAST_GET, DataType.INTEGER)
    def TOTAL_COUNT_OF_RECENT_GET(PQL: PQL): DataCell = DataCell(PQL.dh.TOTAL_COUNT_OF_RECENT_GET, DataType.INTEGER)
    def AFFECTED_ROWS(PQL: PQL): DataCell = DataCell(PQL.AFFECTED_ROWS_OF_LAST_NON_QUERY, DataType.INTEGER)
    def AFFECTED_ROWS_OF_LAST_NON_QUERY(PQL: PQL): DataCell = DataCell(PQL.AFFECTED_ROWS_OF_LAST_NON_QUERY, DataType.INTEGER)
    def AFFECTED_ROWS_OF_LAST_PUT(PQL: PQL): DataCell = DataCell(PQL.dh.AFFECTED_ROWS_OF_LAST_PUT, DataType.INTEGER)
    def TOTAL_AFFECTED_ROWS_OF_RECENT_PUT(PQL: PQL): DataCell = DataCell(PQL.dh.TOTAL_AFFECTED_ROWS_OF_RECENT_PUT, DataType.INTEGER)
    def AFFECTED_ROWS_OF_LAST_PREP(PQL: PQL): DataCell = DataCell(PQL.dh.AFFECTED_ROWS_OF_LAST_PREP, DataType.INTEGER)

    def BOOL(PQL: PQL): DataCell = DataCell(PQL.BOOL, DataType.BOOLEAN)

    def USERID(PQL: PQL): DataCell =  PQL.credential.getCell("userid")
    def USERNAME(PQL: PQL): DataCell = PQL.credential.getCell("username")
    def ROLE(PQL: PQL): DataCell = PQL.credential.getCell("role")
    def USER(PQL: PQL): DataCell = DataCell(PQL.credential, DataType.ROW)

    def RESULT(PQL: PQL): DataCell = {
        PQL.RESULT.lastOption match {
            case Some(v) => DataCell(v)
            case None => DataCell.UNDEFINED
        }
    }
    def BUFFER(PQL: PQL): DataCell = DataCell(PQL.dh.getData, DataType.TABLE)
    def CLEAN_PQL_BODY(PQL: PQL): DataCell = DataCell(PQL.SQL, DataType.TEXT)

    def COOKIES(PQL: PQL): DataCell = DataCell(new Cookies(), DataType.forClassName("io.qross.net.Cookies"))
    def SESSION(PQL: PQL): DataCell = DataCell(new Session(), DataType.forClassName("io.qross.net.Session"))
    def LANGUAGE(PQL: PQL): DataCell = DataCell(Language.name, DataType.TEXT)

    def POINTER(PQL: PQL): DataCell = DataCell(PQL.dh.cursor, DataType.INTEGER)
}