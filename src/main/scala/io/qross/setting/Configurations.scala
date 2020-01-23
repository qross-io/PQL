package io.qross.setting

import io.qross.core.DataRow
import io.qross.jdbc.{DataSource, JDBC}

object Configurations {

    private val CONFIG = new DataRow()

    if (JDBC.hasQrossSystem) {
        DataSource.QROSS.queryDataTable("SELECT conf_key, conf_value FROM qross_conf")
                .foreach(row => {
                    CONFIG.set(row.getString("conf_key"), row.getString("conf_value"))
                }).clear()
    }

    def contains(name: String): Boolean = {
        CONFIG.contains(name)
    }

    def get(name: String): String = {
        CONFIG.getString(name)
    }

    //只更新内存中的配置
    def set(name: String, value: Any): Unit = {
        CONFIG.set(name, value)
    }

    //更新数据库项
    def update(name: String, value: Any): Unit = {
        if (JDBC.hasQrossSystem) {
            DataSource.QROSS.queryUpdate("UPDATE qross_conf SET conf_value=? WHERE conf_key=?", value, name)
        }
    }

    //如果优先从配置项中获取，如果不存在 ，则从Properties中获取
    def getOrProperty(name: String, propertyName: String, defaultValue: String = ""): String = {
        CONFIG.getStringOption(name).getOrElse(Properties.get(propertyName, defaultValue))
    }

    //优先从Properties中获取，如果不存在，则从配置项中获取
    def getPropertyOrElse(propertyName: String, configurationName: String, defaultValue: String = ""): String = {
        if (Properties.contains(propertyName)) {
            Properties.get(propertyName, defaultValue)
        }
        else {
            CONFIG.getStringOption(configurationName).getOrElse(defaultValue)
        }
    }

    //如果配置项不存在 ，则设置默认值
    def getOrElse(name: String, defaultValue: Any): Any = {
        CONFIG.get(name) match {
            case Some(value) => value
            case None => defaultValue
        }
    }
}