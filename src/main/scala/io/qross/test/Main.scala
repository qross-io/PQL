package io.qross.test

import java.io.File
import java.lang.management.ManagementFactory
import java.sql.DriverManager
import java.util.regex.Matcher
import java.util.{Date, Properties}

import io.qross.core._
import io.qross.pql._
import io.qross.fs.{ResourceDir, ResourceFile}
import io.qross.jdbc.DataSource
import io.qross.net.Json
import io.qross.ext.TypeExt._
import io.qross.fs.FilePath._
import io.qross.pql.Solver._
import io.qross.time.{ChronExp, CronExp, DateTime}
import io.qross.pql.Patterns._
import io.qross.pql.PQL._
import io.qross.pql.SAVE$AS.WITH$HEADER$
import io.qross.setting.Environment

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object Main {

    def main(args: Array[String]): Unit = {
        //ResourceDir.open("/api").readAll().foreach(println)

//        val username = "adb_test"
//        val password = "LaKuc9bPzUp4"

//        val connectionProps = new Properties()
//        connectionProps.put("user", username)
//        connectionProps.put("password", password)

//        Class.forName("com.mysql.jdbc.Driver").newInstance()
//        val conn = DriverManager.getConnection(url)
//        val prest = conn.createStatement
//        val rs = prest.executeQuery("select 18 from dual")
//        rs.close()
//        prest.close()
//        conn.close()



        //CronExp.parse("0 0 6-23/1 * * ? *").getNextTick(DateTime.now).print

//        val array = new Array[Class[_]](2)
//        array(0) = Class[DataCell]
//        array(1) = Class[List[DataCell]]

        //new PQL("SELECT switch_time FROM qross_jobs LIMIT 5 -> GET LAST ROW", DataHub.QROSS).run().print

        PQL.openFile("/sql/test.sql").run()


        System.exit(0)

//

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