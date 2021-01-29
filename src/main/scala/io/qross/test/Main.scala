package io.qross.test

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.charset.Charset
import java.sql.DriverManager
import java.time.{LocalDateTime, ZoneId}
import java.util
import java.util.regex.{Matcher, Pattern}
import java.util.Date
import java.util.concurrent.ConcurrentHashMap

import io.qross.app.OneApi
import io.qross.core._
import io.qross.ext.Console
import io.qross.ext.Output.writeLineWithSeal
import io.qross.pql._
import io.qross.fs._
import io.qross.jdbc.DataSource
import io.qross.net.{Email, Http, Json, Redis, Session}
import io.qross.ext.TypeExt._
import io.qross.fs.Path._
import io.qross.pql.Solver._
import io.qross.time.{ChronExp, CronExp, DateTime}
import io.qross.pql.Patterns._
import io.qross.pql.PQL._
import io.qross.security.{Base64, MD5}
import io.qross.setting.{Environment, Global, Language, Properties}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
import io.qross.time.TimeSpan._
import javax.servlet.http.Cookie

import scala.collection.JavaConverters._
import scala.util.Random
import io.qross.fs.TextFile._
import redis.clients.jedis.Protocol.Command

object Main {

    def test(str: String, int: scala.Int): String = {
        str + int
    }

    def main(args: Array[String]): Unit = {


//        val dh = DataHub.QROSS
//
//        dh.get("select * from tc limit 10").show()



        //PQL.runEmbeddedFile("/templates/example.html").print



//             Email.write("HELLO WORLD")
//                .fromTemplate("/templates/example.html")
//                .to("wuzheng@zichan360.com")
//                .send()
//                .print

//        val ds = DataSource.QROSS
//        val table = ds.executeDataTable("SELECT id, title FROM jobs")
//        ds.close()
//        val excel = new Excel("f:/table.xlsx")
//        excel.createSheet("sheet4")
//        excel.cloneSheet("sheet1", "sheet7")
//        excel.dropSheet("sheet3")
//        excel.renameSheet("sheet2", "sheet5")
//        excel.moveSheetTo("sheet4", 4)
//        excel.insert("insert into sheet '2018-2020年Job数据' (A, B) values ('Job ID', 'Job Title')")
//        excel.insert("insert into sheet '2018-2020年Job数据' (A, B) values (#id, '#title')", table)
//        excel.close()

        //val PQL = new PQL("", DataHub.QROSS)
        //"<div><%=@MASTER_LOG_HTML%></div>".replaceSharpExpressions(PQL).popStash(PQL, "").print

//        val str = "HELLX WORLD123 HELLO WORLD JERRY 4567 TOM 12345 JERRY"
//        """[A-Z]+ (WORLD)(?!\d+)""".r.findAllMatchIn(str).foreach(m => println(m.group(0)))

        //"""HELLO(?= TOM)""" => HELLO
        //"""HELLO(?= TOM)""" => HELLO TOM

        //PQL.openFile("/sql/test.sql").place("parent=0&projectName=HELLO").run().print

        //PQL.runEmbedded("<div><%=@MASTER_LOG_HTML%></div>").print

        //List.fill(17)("?").mkString("").print

        //DataSource.QROSS.queryDataTable("select tt from td;").toString().print

        /* PQL.openFile("/sql/api.sql").place(
            "pageSize" -> "20",
            "pageNum" -> "9",
            "month" -> "2020-10",
            "currentPage" -> "2",
            "startDate" -> "2012-02-03",
            "userId" -> "1",
            "reportId" -> "1",
            "stat_date" -> "2020-01-20",
            "debtor_id_number" -> "12",
            "debtor_name" -> "name",
            "telephone" -> "13623456678",
            "companyId" -> "123",
            "collectorIds" -> "1,2,3 a:b:c",
            "statType" -> "1",
            "id" -> "1",
            "time" -> "2020-02-27 18:30:33",
            "filter" -> ""
        ).run().print

        val list = List[java.util.HashMap[String, Int]](
            new java.util.HashMap[String, Int]() {{
                put("a", 1)
                put("b", 2)
            }}, new java.util.HashMap[String, Int]() {{
                put("a", 3)
                put("b", 4)
            }})
         */

        //println(ChronExp("MONTHLY THIRD-SUNDAY 00:00").getNextTick(DateTime.now))

        //new String(redis.jedis.sendCommand(Command.GET, "hello").asInstanceOf[Array[Byte]]).print

        //PQL.openEmbeddedFile("/sql/manual.html").place("jobId=595").set("jobId=595").run().print
        //PQL.openFile("/sql/str.sql").place("id=4&applies=ready,failed").run().print

        //Http.GET("http://127.0.0.1:8848/nacos/v1/cs/configs?dataId=io.qross.config&group=connections").request().print
        //PQL.run("LOAD JSON CONFIG FROM URL 'http://localhost:8848/nacos/v1/cs/configs?dataId=json&group=io.qross.config'")
        //http://localhost:8848/nacos/v1/cs/configs?dataId=test&group=io.qross.config
        //localhost:8848:io.qross.config:test
        PQL.openFile("/sql/test.sql").place(("jobId=4&taskId=2869&recordTime=2020-07-09 22:25:07&position=0&action=0&mode=all")).set("jobId=4&taskId=2869&recordTime=2020-07-09 22:25:07&position=0&action=0&mode=all").run().print


        //PQL.openFile("""C:\io.Qross\Keeper\src\main\resources\pql/keeper_clean.sql""").place("date=2020-06-23&hour=15").run()


//        val writer = new FileWriter("c:/Space/test.log", deleteIfExists = true)
//
//        writer.writeObjectLine(new NoteLogLine("INFO", "愣住你好世界"))
//
//        writer.close()

//        val dh = DataHub.QROSS
//        dh.get("SELECT id, title FROM qross_jobs")
//            .saveAsCsvFile("c:/Space/jobs.csv")
//            .write()
//        dh.close()

        //Directory.list("c:/io.qross/coding").foreach(println)

        //new Sharp("FORMAT yyyy", DataCell(DateTime.now, DataType.DATETIME)).execute().value.print

        //val reader = new TextFile("f:/547.csv", TextFile.CSV)
        //reader.bracketedBy("\"", "\"").limit(10, 1).skipLines(1).execute()
        //reader.cursor.print
        //reader.close()

        //"""\s+[,=]\s+|\s+[,=\)]|[,=\(]\s+""".r.findAllIn("id = 0, title ='HELLO WROLD'").foreach(println)

        //"abc".shuffle(100).print
        //"abcdefghijklmnopqrstuvwxyz0123456789".shuffle(10).print

        //Language.loadAll

//        println(new Session())
//        println(Class.forName("io.qross.net.Cookies")
//                .getDeclaredMethod("getDefault")
//                )
        //, Class.forName("java.lang.String")
            //.invoke(new io.qross.net.Cookies(), "hello & world")

        //val cookie = new Cookie("", "")
        //cookie.setDomain()
        /*
        cookie.setComment()
        cookie.setPath()
        cookie.setHttpOnly()
        cookie.setMaxAge()
        cookie.setSecure()
        cookie.setVersion(int)
        */


//        val row = new DataRow()
//        row.set("r", new DataRow(), DataType.ROW)
//        row.getCell("r").asRow.set("name", "Tom")
//        row.getCell("r").asRow.print

//        PQL.open(
//            """
//               SELECT b, c FROM dt;
//            """.stripMargin).run()

       /*

        OUTPUT "obj" # { "Name": "Tom", "Age": 19 };
        OUTPUT "arr" # [1, 2, 3];
        OUTPUT "val" # "hello" + "#" + "world";

        PQL.open(
            """
              | REQUEST JSON API "http://localhost:8080/api2?name=abc";
              |     PARSE "/";
              |
            """.stripMargin).$return() */

        //ResourceFile.open("/2.sql").exists

        //val row = new DataRow()
        //println(DataType.ofClassName(row.getClass.getName))

    }
}