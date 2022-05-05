package io.qross.pql

import io.qross.core.{DataCell, DataRow, DataType}
import io.qross.exception.SQLExecuteException
import io.qross.fs.TextFile._
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.net.{Cookies, Json, Session}
import io.qross.setting.{Configurations, Environment, Global, Language}
import io.qross.time.DateTime
import io.qross.ext.TypeExt._

object GlobalVariable {

    //除环境全局变量的其他全局变量

    //系统和用户全局变量的存在依赖于进程，静态变量只有在进程启动时才会运行，所以手工更新数据库的值并不会生效

    //系统全局变量, 仅作用于当前 PQL
    val SYSTEM: DataRow = new DataRow()
    //用户全局变量
    val USER: DataRow = new DataRow()

    //Global.getClass.getDeclaredMethods.contains()
    //Global.getClass.getMethod("").invoke(null)

    //设置临时系统变量, 系统启动时更新, 关闭时删除, 只作用于当前系统
    //如果设置了与更高级的全局变量相同的变量名, 无意义
    def set(name: String, value: Any): Unit = {
        SYSTEM.set(name, value)
    }

    //更新用户变量
    def set(name: String, value: DataCell, userid: Int): Unit = {

        if (Patterns.GLOBAL_VARIABLES.contains(name)) {
            throw new SQLExecuteException("Can't update process variable. This variable is readonly.")
        }
        else if (GlobalFunction.NAMES.contains(name)) {
            throw new SQLExecuteException(s"$name is a global function.")
        }
        else if (Configurations.contains(name)) {
            Configurations.set(name, value.value)
        }
        else if (SYSTEM.contains(name) || userid == 0) {
            SYSTEM.set(name, value)
            if (JDBC.hasQrossSystem) {
                val ds = DataSource.QROSS
                if (ds.tableExists("qross_variables")) {
                    val field = value.toString
                    ds.executeNonQuery(s"""INSERT INTO qross_variables (variable_group, variable_type, owner, variable_name, variable_value) VALUES ('system', ?, 0, ?, ?) ON DUPLICATE KEY UPDATE variable_value=?""",
                        value.dataType.typeName, name, field, field)
                }
                ds.close()
            }
        }
        else {
            USER.set(name, value)
            if (JDBC.hasQrossSystem) {
                val ds = DataSource.QROSS
                if (ds.tableExists("qross_variables")) {
                    val field = value.toString
                    ds.executeNonQuery(s"""INSERT INTO qross_variables (variable_group, variable_type, owner, variable_name, variable_value) VALUES ('user', ?, ?, ?, ?) ON DUPLICATE KEY UPDATE variable_value=?""",
                        value.dataType.typeName, userid, name, field, field)
                }
                ds.close()
            }
        }
    }

    //得到全局变量的值
    def get(name: String, PQL: PQL): DataCell = {
        if (Patterns.GLOBAL_VARIABLES.contains(name)) {
            Class.forName("io.qross.pql.GlobalVariableDeclaration")
                .getDeclaredMethod(name, Class.forName("io.qross.pql.PQL"))
                .invoke(null, PQL)
                .asInstanceOf[DataCell]
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
        else if (GlobalFunction.WITHOUT_ARGUMENTS.contains(name)) {
            GlobalFunction.call(name)
        }
        else if (Configurations.contains(name)) {
            DataCell(Configurations.getOrProperty(name.toUpperCase, name.replace("_", ".").toLowerCase(), null))
        }
        else {
            DataCell.UNDEFINED
        }
    }

    def contains(name: String, PQL: PQL): Boolean = {
        Patterns.GLOBAL_VARIABLES.contains(name) || USER.contains(name) || SYSTEM.contains(name) || PQL.credential.contains(name) || Configurations.contains(name)
    }

    def renew(name: String, owner: Int = 0): Unit = {
        val row = DataSource.QROSS.queryDataRow("SELECT variable_type, variable_value FROM qross_variables WHERE variable_name=? AND owner=?", name, owner)
        if (owner == 0) {
            SYSTEM.set(name, row.getString("variable_type") match {
                case "TEXT" => row.getString("variable_value")
                case "INTEGER" => row.getLong("variable_value")
                case "DECIMAL" => row.getDouble("variable_value")
                case "BOOLEAN" => row.getBoolean("variable_value")
                case "ARRAY" => Json.fromText(row.getString("variable_value")).parseJavaList("/")
                case "ROW" => Json.fromText(row.getString("variable_value")).parseRow("/")
                case "TABLE" => Json.fromText(row.getString("variable_value")).parseTable("/")
                case _ => row.getString("variable_value")
            }, new DataType(row.getString("variable_type")))
        }
        else {
            USER.set(name, row.getString("variable_type") match {
                case "TEXT" => row.getString("variable_value")
                case "INTEGER" => row.getLong("variable_value")
                case "DECIMAL" => row.getDouble("variable_value")
                case "BOOLEAN" => row.getBoolean("variable_value")
                case "ARRAY" => Json.fromText(row.getString("variable_value")).parseJavaList("/")
                case "ROW" => Json.fromText(row.getString("variable_value")).parseRow("/")
                case "TABLE" => Json.fromText(row.getString("variable_value")).parseTable("/")
                case _ => row.getString("variable_value")
            }, new DataType(row.getString("variable_type")))
        }
    }

    def remove(name: String, owner: Int = 0): Unit = {
        if (owner == 0) {
            SYSTEM.remove(name)
        }
        else {
            USER.remove(name)
        }
    }
}

object GlobalVariableDeclaration {

    def ARGUMENTS(PQL: PQL): DataCell = DataCell(PQL.ARGUMENTS, DataType.ROW)

    def TODAY(PQL: PQL): DataCell = DataCell(DateTime.now.setZeroOfDay(), DataType.DATETIME)
    def YESTERDAY(PQL: PQL): DataCell = DataCell(DateTime.now.minusDays(1).setZeroOfDay(), DataType.DATETIME)
    def TOMORROW(PQL: PQL): DataCell = DataCell(DateTime.now.plusDays(1).setZeroOfDay(), DataType.DATETIME)
    def NOW(PQL: PQL): DataCell = DataCell(DateTime.now, DataType.DATETIME)
    def TIMESTAMP(PQL: PQL): DataCell = DataCell(DateTime.now.toEpochSecond, DataType.INTEGER)

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

    def USERID(PQL: PQL): DataCell =  PQL.credential.getCell("userid")
    def USERNAME(PQL: PQL): DataCell = PQL.credential.getCell("username")
    def ROLE(PQL: PQL): DataCell = PQL.credential.getCell("role")
    def USER(PQL: PQL): DataCell = DataCell(PQL.credential, DataType.ROW)

    def BUFFER(PQL: PQL): DataCell = DataCell(PQL.dh.getData, DataType.TABLE)
    def CLEAN_PQL_BODY(PQL: PQL): DataCell = DataCell(PQL.SQL, DataType.TEXT)

    def COOKIES(PQL: PQL): DataCell = DataCell(new Cookies(), DataType.forClassName("io.qross.net.Cookies"))
    def SESSION(PQL: PQL): DataCell = DataCell(new Session(), DataType.forClassName("io.qross.net.Session"))
    def LANGUAGE(PQL: PQL): DataCell = DataCell(Language.name, DataType.TEXT)

    def POINTER(PQL: PQL): DataCell = DataCell(PQL.dh.cursor, DataType.INTEGER)

    def THEME(PQL: PQL): DataCell = DataCell(io.qross.look.Theme.random, DataType.ROW)

    def RUNNING_DIR(PQL: PQL): DataCell = DataCell(Environment.runningDirectory, DataType.TEXT)
    def LOCAL_IP(PQL: PQL): DataCell = DataCell(Environment.localHostAddress, DataType.TEXT)

    def KEEPER_IS_RUNNING(PQL: PQL): DataCell = DataCell(DataSource.QROSS.queryExists("SELECT id FROM qross_keeper_beats WHERE actor_name='Keeper' AND `status`='running' AND timestampdiff(second, last_beat_time, now())<120"), DataType.BOOLEAN)
    def KEEPER_HTTP_SERVICE(PQL: PQL): DataCell = DataSource.QROSS.executeSingleValue("""SELECT A.node_address AS service FROM (SELECT node_address, busy_score FROM  qross_keeper_nodes WHERE status='online' AND disconnection=0) A
                            INNER JOIN (SELECT node_address FROM qross_keeper_beats WHERE actor_name='Keeper' AND `status`='running' AND timestampdiff(second, last_beat_time, now())<120) B
                            ON A.node_address=B.node_address ORDER BY A.busy_score ASC LIMIT 1""").asText("").toDataCell(DataType.TEXT)

    def PI(PQL: PQL): DataCell = DataCell(Math.PI, DataType.DECIMAL)
}