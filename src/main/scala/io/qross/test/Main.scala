package io.qross.test

import java.io.File
import java.lang.management.ManagementFactory
import java.sql.DriverManager
import java.util.regex.{Matcher, Pattern}
import java.util.{Date, Properties}

import io.qross.core._
import io.qross.pql._
import io.qross.fs.{Excel, FileReader, ResourceDir, ResourceFile}
import io.qross.jdbc.DataSource
import io.qross.net.{Email, Json}
import io.qross.ext.TypeExt._
import io.qross.fs.Path._
import io.qross.pql.Solver._
import io.qross.time.{ChronExp, CronExp, DateTime}
import io.qross.pql.Patterns._
import io.qross.pql.PQL._
import io.qross.setting.{Environment, Global}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
import io.qross.time.TimeSpan._

object Main {

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

        PQL.openFile("/sql/test.sql").place("parent", "w").run().print
        //PQL.openEmbeddedFile("/sql/test.html").place("jobId", "54").run().print

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