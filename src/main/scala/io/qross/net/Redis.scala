package io.qross.net

import io.qross.core.{DataCell, DataHub}
import io.qross.exception.ExtensionNotFoundException
import io.qross.setting.Properties
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException

object Redis {

    def open(serverName: String): Jedis = {
        if (Properties.contains(s"redis.$serverName.host")) {
            val host = Properties.get(s"redis.$serverName.host")
            val port = Properties.get(s"redis.$serverName.port", "6379").toInt

            val password = Properties.get(s"redis.$serverName.password", "")
            val database = Properties.get(s"redis.$serverName.database", "0").toInt

            val CONNECTION_TIMEOUT: Int = 300000
            val SOCKET_TIMEOUT: Int = 300000

            val jedis = new Jedis(host, port, CONNECTION_TIMEOUT, SOCKET_TIMEOUT)
            if (password != "") {
                jedis.auth(password)
            }
            if (database > 0) {
                jedis.select(database)
            }

            jedis
        }
        else {
            throw new JedisConnectionException("Incorrect Redis server name: " + serverName)
        }
    }

    implicit class DataHub$Redis(dh: DataHub) {

        def REDIS: Jedis = {
            dh.pick[Jedis]("REDIS") match {
                case Some(redis) => redis
                case None => throw new ExtensionNotFoundException("Must open a redis host first.")
            }
        }

        def openRedis(serverName: String): DataHub = {
            if (dh.slots("REDIS$R")) {
                dh.pick[Jedis]("REDIS$R") match {
                    case Some(jedis) => jedis.close()
                    case None =>
                }
            }
            dh.plug("REDIS$R", Redis.open(serverName))
        }

        def saveToRedis(serverName: String): DataHub = {
            if (dh.slots("REDIS$W")) {
                dh.pick[Jedis]("REDIS$W") match {
                    case Some(jedis) => jedis.close()
                    case None =>
                }
            }
            dh.plug("REDIS$W", Redis.open(serverName))
        }

        //运行单条语句, 如 GET key 或 SET key value
        def run(sentence: String): DataCell = {
            null
        }

        def buffer(sentence: String): DataHub = {
            dh
        }

        //批量执行, 批量更新
        def pipelined(sentence: String): DataHub = {
            //key -> value
            //item
            dh
        }
    }
}

class Redis(val serverName: String) {

    val jedis: Jedis = Redis.open(serverName)


    def run(command: String, key: String, value: Any*): DataCell = {
        null
    }

    def pipelined(command: String, key: String, value: Any*): Redis = {

        this
    }

    def sync(): Unit = {

    }

    def close(): Unit = {
        jedis.close()
    }
}