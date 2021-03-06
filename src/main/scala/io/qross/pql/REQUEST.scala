package io.qross.pql

import io.qross.ext.TypeExt._
import io.qross.net.Http
import io.qross.net.Json._
import io.qross.pql.Patterns._
import io.qross.pql.Solver._

object REQUEST {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("REQUEST", sentence, new REQUEST(sentence.takeAfterX($REQUEST))))
    }
}

class REQUEST(private val sentence: String) {

    def execute(PQL: PQL): Unit = {

        val plan = Syntax("REQUEST").plan(sentence.$restore(PQL))

        plan.head match {
            case "JSON API" =>
                val url = plan.headArgs.replace(" ", "%20")
                val method = plan.get("METHOD", "USE METHOD").getOrElse("GET").removeQuotes().toUpperCase()
                val data = plan.oneArgs("DATA", "SEND DATA")
                val header = plan.mapArgs("HEADER", "SET HEADER")

                val http: Http =
                    method match {
                        case "GET" => Http.GET(url)
                        case "POST" => Http.POST(url, data)
                        case "PUT" => Http.PUT(url, data)
                        case "DELETE" => Http.DELETE(url, data)
                        case _ => new Http(method, url, data)
                    }

                if (header.nonEmpty) {
                    for ((k, v) <- header) {
                        http.setHeader(k, v)
                    }
                }

                PQL.dh.openJson(http.request())
            case _ =>
        }
    }
}