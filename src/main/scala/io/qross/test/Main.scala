package io.qross.test

import java.sql.DriverManager
import java.util.{Date, Properties}

import io.qross.core._
import io.qross.sql.Solver
import io.qross.fs.{ResourceDir, ResourceFile}
import io.qross.jdbc.DataSource
import io.qross.net.Json
import io.qross.ext.TypeExt._
import io.qross.sql.Solver._
import io.qross.sql.{OUTPUT, PSQL, Patterns, SHARP}
import io.qross.time.{CronExp, DateTime}
import io.qross.sql.Patterns._
import io.qross.sql.PSQL._

import scala.collection.mutable
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

        PSQL.runFile("/sql/test.sql")

        System.exit(0)

        val m = $EXIT.matcher("exit when true")
        if (m.find()) {
            m.group(0).print
            println(m.group(1) == "")
        }



        System.exit(0)

        new PSQL("""12"3\",4" --"--"hello
         | 5678',90'
         | /*
         | '--\'na'me
         | HELLO WORLD
         | */
         | """.stripMargin, new DataHub())


        val SQL = """
                     SET $rds := "mysql.rds";
                     OPEN $rds:
                        GET # SELECT
               OUTPUT VALUE # {
                     "test": $value{ SELECT 'A' as c FROM dt }
                 }

                 # DEFAULT 0;
            """

        val dh = new DataHub()

        dh.run(SQL)

        dh.close()
//

//        PSQL.open(
//            """
//               SELECT b, c FROM dt;
//            """.stripMargin).run()


       /*

               OUTPUT "obj" # { "Name": "Tom", "Age": 19 };
        OUTPUT "arr" # [1, 2, 3];
        OUTPUT "val" # "hello" + "#" + "world";

        PSQL.open(
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