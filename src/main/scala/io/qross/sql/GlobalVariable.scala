package io.qross.sql

import io.qross.core.{DataCell, DataRow, DataType}
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.setting.{Configurations, Global}
import io.qross.time.DateTime

object GlobalVariable {

    val GLOBALS: Set[String] = Global.getClass.getDeclaredMethods.map(_.getName).toSet
    //除环境全局变量的其他全局变量
    val SYSTEM: DataRow = new DataRow()
    val USER: DataRow = new DataRow()
    val PROCESS: Set[String] = Set("NOW", "TODAY", "YESTERDAY", "COUNT", "TOTAL", "ROWS", "AFFECTED", "USER_ID", "USERNAME", "ROLE")

    //Global.getClass.getDeclaredMethods.contains()
    //Global.getClass.getMethod("").invoke(null)

    if (JDBC.hasQrossSystem) {
        DataSource.queryDataTable("SELECT var_group, var_name, var_type, var_value FROM qross_variables WHERE var_group='SYSTEM'")
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

    //更新用户变量
    def set(name: String, value: Any, user: Int = 0, role: String = "WORKER"): Unit = {

        if (PROCESS.contains(name)) {
            throw new SQLExecuteException("Can't update process variable. This variable is readonly.")
        }
        else if (SYSTEM.contains(name) || Configurations.contains(name)) {
            if (role == "MASTER") {
                if (SYSTEM.contains(name)) {
                    SYSTEM.set(name, value)
                    //更新数据库
                    DataSource.queryUpdate(s"""INSERT INTO qross_variables (var_group, var_type, var_user, var_name, var_value) VALUES ('SYSTEM', ?, 0, ?, ?) ON DUPLICATE KEY UPDATE var_value=?""", value match {
                        case i: Int => "INTEGER"
                        case l: Long => "INTEGER"
                        case f: Float => "DECIMAL"
                        case d: Double => "DECIMAL"
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
                DataSource.queryUpdate(s"""INSERT INTO qross_variables (var_group, var_type, var_user, var_name, var_value) VALUES ('USER', ?, ?, ?, ?) ON DUPLICATE KEY UPDATE var_value=?""", value match {
                    case i: Int => "INTEGER"
                    case l: Long => "INTEGER"
                    case f: Float => "DECIMAL"
                    case d: Double => "DECIMAL"
                    case _ => "TEXT"
                }, user, name, value, value)
            }
        }
    }

    //得到全局变量的值
    def get(name: String, PSQL: PSQL ): DataCell = {
        if (PROCESS.contains(name)) {
            name match {
                case "TODAY" => DataCell(DateTime.now.setZeroOfDay(), DataType.DATETIME)
                case "YESTERDAY" => DataCell(DateTime.now.minusDays(1).setZeroOfDay(), DataType.DATETIME)
                case "NOW" =>  DataCell(DateTime.now, DataType.DATETIME)
                case "COUNT" => DataCell(PSQL.dh.COUNT, DataType.INTEGER)
                case "TOTAL" => DataCell(PSQL.dh.TOTAL, DataType.INTEGER)
                case "ROWS" => DataCell(PSQL.ROWS, DataType.INTEGER)
                case "AFFECTED" => DataCell(PSQL.AFFECTED, DataType.INTEGER)
                case "USER_ID" => DataCell(PSQL.dh.userId, DataType.INTEGER)
                case "USERNAME" => DataCell(PSQL.dh.userName, DataType.TEXT)
                case "ROLE" => DataCell(PSQL.dh.roleName, DataType.TEXT)
                case _ => DataCell.NOT_FOUND
            }
        }
        else if (USER.contains(name)) {
            USER.getCell(name)
        }
        else if (SYSTEM.contains(name)) {
            SYSTEM.getCell(name)
        }
        else if (GLOBALS.contains(name)) {
            DataCell(Class.forName("io.qross.setting.Global").getDeclaredMethod(name).invoke(null))
        }
        else {
            DataCell.NOT_FOUND
        }
    }
}
