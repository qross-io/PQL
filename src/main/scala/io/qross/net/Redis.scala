package io.qross.net

import io.qross.core.Parameter._
import io.qross.core._
import io.qross.exception.{ExtensionNotFoundException, SQLParseException}
import io.qross.ext.TypeExt._
import io.qross.setting.Properties
import redis.clients.jedis.Protocol.Command
import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.mutable

object Redis {

    //命令列表, 用于 sendCommand 的第一个参数
    val COMMANDS: Map[String, Command] =
        Class.forName("redis.clients.jedis.Protocol$Command")
            .getEnumConstants
            .map(`enum` => (`enum`.toString, `enum`.asInstanceOf[Command]))
            .toMap[String, Command]
    //返回DataRow/Map类型的命令列表
    val MAPS: Set[String] = Set[String]("HGETALL", "ZRANGEBYSCORE", "ZRANGE", "ZREVRANGE", "ZREVRANGEBYSCORE")
    val TABLES: Set[String] = Set[String]("GEOPOS", "GEORADIUS", "GEOHASH")

    def open(serverName: String, defaultDatabase: Int = -1): Jedis = {
        if (Properties.contains(s"redis.$serverName.host")) {
            val host = Properties.get(s"redis.$serverName.host")
            val port = Properties.get(s"redis.$serverName.port", "6379").toInt

            val password = Properties.get(s"redis.$serverName.password", "")
            val database = if (defaultDatabase > -1) defaultDatabase else Properties.get(s"redis.$serverName.database", "0").toInt

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

        def REDIS$R: Redis = {
            dh.pick[Redis]("REDIS$R") match {
                case Some(redis) => redis
                case None => throw new ExtensionNotFoundException("Must open a Redis host first.")
            }
        }

        def REDIS$W: Redis = {
            dh.pick[Redis]("REDIS$W") match {
                case Some(redis) => redis
                case None => throw new ExtensionNotFoundException("Must save to a Redis host first.")
            }
        }

        def openRedis(serverName: String, defaultDatabase: Int = -1): DataHub = {
            if (dh.slots("REDIS$R")) {
                dh.pick[Redis]("REDIS$R") match {
                    case Some(redis) => redis.close()
                    case None =>
                }
            }
            if (!dh.slots("REDIS$W")) {
                dh.plug("REDIS$W", new Redis(serverName, defaultDatabase))
            }
            dh.plug("REDIS$R", new Redis(serverName, defaultDatabase))
        }

        def select(databaseIndex: Int): DataHub = {
            REDIS$R.select(databaseIndex)
            dh
        }

        def saveToRedis(serverName: String, defaultDatabase: Int = -1): DataHub = {
            if (dh.slots("REDIS$W")) {
                dh.pick[Redis]("REDIS$W") match {
                    case Some(redis) => redis.close()
                    case None =>
                }
            }
            dh.plug("REDIS$W", new Redis(serverName, defaultDatabase))
        }

        def switch(databaseIndex: Int): DataHub = {
            REDIS$W.select(databaseIndex)
            dh
        }

        //在OPEN源中运行语句用 SET
        //在SAVE源中运行语句用 prep

        //只支持获取数据的语句, 将获得的数据缓存为一列或两列的表格, 列名一列固定为value，两列固定为key和value
        //如果返回的只是一个值, 则表格只有一行
        //如果需要行表，则继续通过列转行方法转制
        //可以理解为是redis的get
        // stock (存货, 备料) 与 pipelined (流水线) 对应
        def stock(sentence: String): DataHub = {
            dh.buffer(REDIS$R.command(sentence).asTable)
        }

        // 功能同 pass, 无参时可以代替stock
        def transfer(sentence: String): DataHub = {
            if (sentence.hasParameters) {
                val params = sentence.pickParameters()
                dh.getData.foreach(row => {
                    REDIS$R.pipelined(sentence.replaceParameters(params, row))
                })
                dh.clear().buffer(REDIS$R.syncAndReturnAll())
            }
            else {
                stock(sentence)
            }
        }

        //批量执行, 批量更新
        //功能同put, 支持 # 和 & 占位符
        //在不确定值是否有空格的情况下, 建议全部加上引号
        def pipelined(sentence: String): DataHub = {
            if (sentence.hasParameters) {
                val params = sentence.pickParameters()
                dh.getData.foreach(row => {
                    REDIS$W.pipelined(sentence.replaceParameters(params, row))
                })
                REDIS$W.sync()
            }
            else {
                REDIS$W.command(sentence)
            }
            dh
        }

        //可以  buffer -> put 或 get -> pipelined 交叉使用

        //运行单条语句并返回值
        def command(sentence: String): DataCell = {
            REDIS$R.command(sentence)
        }

        //运行多条语句并返回值
        def pipelined(sentence: String, table: DataTable): DataTable = {
            if (sentence.hasParameters) {
                val params = sentence.pickParameters()
                table.foreach(row => {
                    REDIS$R.pipelined(sentence.replaceParameters(params, row))
                })
                REDIS$R.syncAndReturnAll()
            }
            else {
                command(sentence).asTable
            }
        }
    }
}

class RedisCommand(sentence: String) {
    private val hasChars = sentence.contains("'") || sentence.contains("\"")
    private val chars = new mutable.ListBuffer[String]()

    private val parts = sentence.trim().takeAfter("""(?i)REDIS\s""".r).pickChars(chars).trim().split("""\s+""")

    val name: String = parts(0).toUpperCase()
    val command: Command = {
        if (Redis.COMMANDS.contains(name)) {
            Redis.COMMANDS(name)
        }
        else {
            throw new SQLParseException("Incorrect redis command: " + parts(0))
        }
    }

    val args: Array[String] = {
            if (hasChars) {
                parts.drop(1).map(_.restoreChars(chars).removeQuotes())
            }
            else {
                parts.drop(1)
            }
        }
}

class Redis(val serverName: String, val defaultDatabase: Int) {

    def this(serverName: String) {
        this(serverName, -1)
    }

    val jedis: Jedis = Redis.open(serverName, defaultDatabase)
    private val reservoir = mutable.ArrayBuffer[mutable.ArrayBuffer[RedisCommand]](
        new mutable.ArrayBuffer[RedisCommand]()
    )

    def select(databaseIndex: Int): Redis = {
        jedis.select(databaseIndex)
        this
    }

    def eval(value: Any, rco: RedisCommand): DataCell = {
        if (value != null) {
            if (Redis.MAPS.contains(rco.name)) {
                val row = new DataRow()
                val list = value.asInstanceOf[java.util.ArrayList[Array[Byte]]]
                for (i <- 0 until list.size()) {
                    if (i % 2 == 0 && i + 1 < list.size()) {
                        row.set(new String(list.get(i)), new String(list.get(i+1)))
                    }
                }
                DataCell(row, DataType.ROW)
            }
            else if (Redis.TABLES.contains(rco.name)) {
                val table = new DataTable()
                if (rco.name == "GEOPOS") {
                    val geo = value.asInstanceOf[java.util.ArrayList[java.util.ArrayList[Array[Byte]]]]
                    for (i <- 0 until geo.size()) {
                        val row = new DataRow()
                        row.set("site", rco.args(i + 1))
                        if (geo.get(i) != null) {
                            row.set("x", new String(geo.get(i).get(0)).toDouble, DataType.DECIMAL)
                            row.set("y", new String(geo.get(i).get(1)).toDouble, DataType.DECIMAL)
                        }
                        else {
                            row.set("x", null, DataType.DECIMAL)
                            row.set("y", null, DataType.DECIMAL)
                        }
                        table.addRow(row)
                    }
                }
                else if (rco.name == "GEORADIUS") {
                    val largs = rco.args.map(_.toLowerCase()).toSet
                    val (dist: Int, hash: Int) = {
                        if (largs.contains("withdist")) {
                            if (largs.contains("withhash")) {
                                (1, 2)
                            }
                            else {
                                (1, -1)
                            }
                        }
                        else if (largs.contains("withhash")) {
                            (-1, 1)
                        }
                        else {
                            (-1, -1)
                        }
                    }
                    val coord: Int = if (largs.contains("withcoord")) 3 else -1

                    if (dist > 0 || hash > 0 || coord > 0) {
                        val geo = value.asInstanceOf[java.util.ArrayList[java.util.ArrayList[Any]]]
                        for (i <- 0 until geo.size()) {
                            val row = new DataRow()
                            row.set("site", new String(geo.get(i).get(0).asInstanceOf[Array[Byte]]), DataType.TEXT)
                            if (dist > 0) {
                                row.set("dist", new String(geo.get(i).get(dist).asInstanceOf[Array[Byte]]).toDouble, DataType.DECIMAL)
                            }
                            if (hash > 0) {
                                row.set("hash", geo.get(i).get(hash).asInstanceOf[Long], DataType.INTEGER)
                            }
                            if (coord > 0) {
                                val coord = geo.get(i).get(geo.get(i).size() - 1).asInstanceOf[java.util.ArrayList[Array[Byte]]]
                                row.set("x", new String(coord.get(0)).toDouble, DataType.DECIMAL)
                                row.set("y", new String(coord.get(1)).toDouble, DataType.DECIMAL)
                            }
                            table.addRow(row)
                        }
                    }
                    else {
                        val geo = value.asInstanceOf[java.util.ArrayList[Array[Byte]]]
                        for (i <- 0 until geo.size()) {
                            val row = new DataRow()
                            row.set("site", new String(geo.get(i)), DataType.TEXT)
                            table.addRow(row)
                        }
                    }
                }
                else if (rco.name == "GEOHASH") {
                    val geo = value.asInstanceOf[java.util.ArrayList[Array[Byte]]]
                    for (i <- 0 until geo.size()) {
                        val row = new DataRow()
                        row.set("site", rco.args(i + 1))
                        if (geo.get(i) != null) {
                            row.set("hash", new String(geo.get(i)), DataType.TEXT)
                        }
                        else {
                            row.set("hash", null, DataType.TEXT)
                        }
                        table.addRow(row)
                    }
                }

                DataCell(table, DataType.TABLE)
            }
            else {
//                if (value.isInstanceOf[Array[Byte]]) {
//                    new String(value.asInstanceOf[Array[Byte]]).toDataCell(DataType.TEXT)
//                }
//                else if (value.isInstanceOf[java.util.List[Any]]) {
//                    val array = new java.util.ArrayList[String]()
//                    val list = value.asInstanceOf[java.util.List[Any]]
//                    for (i <- 0 until list.size()) {
//                        array.add(new String(list.get(i).asInstanceOf[Array[Byte]]))
//                    }
//                    DataCell(array, DataType.ARRAY)
//                }
//                else {
//                    DataCell(value)
//                }
                value match {
                    case bytes: Array[Byte] => new String(bytes).toDataCell(DataType.TEXT)
                    // List 和 ArrayList 均可
                    case list: java.util.List[Array[Byte]@unchecked] =>
                        val array = new java.util.ArrayList[String]()
                        for (i <- 0 until list.size()) {
                            array.add(new String(list.get(i)))
                        }
                        DataCell(array, DataType.ARRAY)
                        //list.asScala.map(item => new String(item.asInstanceOf[Array[Byte]])).asJava.toDataCell(DataType.ARRAY)
                    case _ => DataCell(value)  //long, double
                }
            }
        }
        else {
            DataCell.NULL
        }
    }

    def command(sentence: String): DataCell = {
        val rco = new RedisCommand(sentence)
        val value = jedis.sendCommand(rco.command, rco.args: _*)

        eval(value, rco)
    }

    def run(command: String): Any = {
        val rco = new RedisCommand(command)
        val value = jedis.sendCommand(rco.command, rco.args: _*)

        eval(value, rco).value
    }

    def pipelined(sentence: String): Redis = {
        reservoir.last += new RedisCommand(sentence)
        if (reservoir.last.size >= 10000) {
            reservoir += new mutable.ArrayBuffer[RedisCommand]()
        }
        this
    }

    def sync(): Redis = {
        //每10000行提交一次
        reservoir.foreach(block => {
            val pipeline: Pipeline = jedis.pipelined()
            block.foreach(rco => {
                pipeline.sendCommand(rco.command, rco.args: _*)
            })
            pipeline.sync()
            pipeline.close()
        })

        reservoir.clear()
        reservoir += new mutable.ArrayBuffer[RedisCommand]()

        this
    }

    def syncAndReturnAll(): DataTable = {
        val pipeline: Pipeline = jedis.pipelined()
        reservoir.foreach(_.foreach(
            rco => {
                pipeline.sendCommand(rco.command, rco.args: _*)
            }
        ))

        val list = pipeline.syncAndReturnAll()
        val table = new DataTable()
        for (i <- 0 until list.size()) {
            val value = eval(list.get(i), reservoir(i / 10000)(i % 10000))
            if (value.nonNull) {
                table.merge(value.asTable)
            }
        }

        list.clear()
        pipeline.close()
        reservoir.clear()
        reservoir += new mutable.ArrayBuffer[RedisCommand]()

        table
    }

    def close(): Unit = {
        jedis.close()
    }
}