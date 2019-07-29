package io.qross.sql

import io.qross.time.DateTime

import scala.collection.mutable

//暂时无用

object ResultCache {

    private val ALL = new mutable.HashMap[String, mutable.HashMap[String, Any]]
    private val EXPIRE = new mutable.HashMap[String, mutable.HashMap[String, Long]]

    private var lastHour = DateTime.now.getHour

    def set(apiName: String, value: Any): Unit = {
        set(apiName, "", value)
    }

    def set(apiName: String, params: String, value: Any): Unit = {
        if (!ALL.contains(apiName)) {
            ALL.put(apiName, new mutable.HashMap[String, Any])
            EXPIRE.put(apiName, new mutable.HashMap[String, Long])
        }
        ALL(apiName).put(params, value)
        EXPIRE(apiName).put(params, DateTime.now.toEpochSecond)
    }


    def get(apiName: String, params: String = ""): Option[Any] = {
        if (ALL.contains(apiName) && ALL(apiName).contains(params)) {
            if (!EXPIRE.contains(apiName)) {
                EXPIRE.put(apiName, new mutable.HashMap[String, Long])
            }
            EXPIRE(apiName).put(params, DateTime.now.toEpochSecond)
            Some(ALL(apiName)(params))
        }
        else {
            None
        }
    }

    def contains(apiName: String, params: String = ""): Boolean = {
        ResultCache.clean()
        ALL.contains(apiName) && ALL(apiName).contains(params)
    }

    def remove(apiName: String): Unit = {
        ALL.remove(apiName)
        EXPIRE.remove(apiName)
    }

    def remove(apiName: String, params: String): Unit = {
        if (ALL.contains(apiName) && ALL(apiName).contains(params)) {
            ALL(apiName).remove(params)
            EXPIRE(apiName).remove(params)

            if (EXPIRE(apiName).isEmpty) {
                EXPIRE.remove(apiName)
            }
            if (ALL(apiName).isEmpty) {
                ALL.remove(apiName)
            }
        }
    }

    def clean(): Unit = {
        val now = DateTime.now
        val hour = now.getHour
        if (hour != lastHour) {
            val second = now.toEpochSecond
            //待清除列表
            val nps = new mutable.HashMap[String, mutable.ArrayBuffer[String]]()
            for (name <- EXPIRE.keySet) {
                for (params <- EXPIRE(name).keySet) {
                    if (second - EXPIRE(name)(params) >= 3600) {
                        if (!nps.contains(name)) {
                            nps += name -> new mutable.ArrayBuffer[String]()
                        }
                        nps(name) += params
                    }
                }
            }

            for (n <- nps.keySet) {
                for (p <- nps(n)) {
                    remove(n, p)
                }
            }

            nps.clear()
            lastHour = hour
        }
    }
}


case class NameAndParams(var name: String, var params: String)