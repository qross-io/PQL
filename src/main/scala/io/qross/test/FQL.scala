package io.qross.test

import io.qross.fql.SELECT
import io.qross.script.Shell._
import io.qross.script.Shell
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns

import scala.sys.process._

object FQL {
    def main(args: Array[String]): Unit = {
        //new SELECT("select * from :hello where name='Tom' and age>18 order by age desc limit 10")

        //ps$ef grep
        //ps$ef id
        //kill
        //kill id
        //destroy
        //end

        println(Patterns.$RETURN.test("ReTURN # hello"))

        val str = "hello)world"
        val half = str.indexHalfOf('(', ')')
        println(str.takeAfter(")"))

        if (args.nonEmpty) {
            args(0) match {
                case "ps" =>
                    ps$ef("qross-test").foreach(println)
                case "destroy" =>
                    println(destroy("test.sh"))
                case "end" =>
                    println(end("test.sh"))
                case _ =>
                    println(kill(args(0).toInt))
            }
        }
        else {
            println("LACK OF ARGS.")
        }
    }
}