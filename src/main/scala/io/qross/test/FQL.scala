package io.qross.test

import io.qross.fql.SELECT

object FQL {
    def main(args: Array[String]): Unit = {
        new SELECT("select * from :hello where name='Tom' and age>18 order by age desc limit 10")
    }
}
