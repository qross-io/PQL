package io.qross.core

import io.qross.jdbc.{DataSource, JDBC}
import io.qross.pql.GlobalVariable

object Authentication {
    implicit class DataHub$Auth(val dh: DataHub) {

        def userId: Int = {
            if (dh.slots("USER_ID")) {
                dh.pick("USER_ID").asInstanceOf[Int]
            }
            else {
                0
            }
        }

        def userName: String = {
            if (dh.slots("USER_NAME")) {
                dh.pick("USER_NAME").asInstanceOf[String]
            }
            else {
                "anonymous"
            }
        }

        def roleName: String = {
            if (dh.slots("ROLE_NAME")) {
                dh.pick("ROLE_NAME").asInstanceOf[String]
            }
            else {
                "WORKER"
            }
        }

        def signIn(userId: Int, userName: String, roleName: String = "WORKER"): DataHub = {
            dh.plug("USER_ID", userId)
            dh.plug("USER_NAME", userName)
            dh.plug("ROLE_NAME", roleName)

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

            dh
        }
    }
}
