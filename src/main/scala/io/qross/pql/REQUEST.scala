package io.qross.pql

import java.util.regex.Matcher

import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.net.Http
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
import io.qross.net.Json._

object REQUEST {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($REQUEST.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("REQUEST", sentence, new REQUEST(sentence.takeAfter($REQUEST).trim())))
        }
        else {
            throw new SQLParseException("Incorrect REQUEST JSON API sentence: " + sentence)
        }
    }
}

class REQUEST(var sentence: String) {

    private var m: Matcher = _

    val header: Map[String, String] = if ({ m = $REQUEST$HEADER.matcher(sentence); m}.find) {
        sentence = sentence.replace(m.group(0), "").trim()
        m.group(1).$split()
    }
    else {
        Map[String, String]()
    }

    val (method: String, data: String) = if ({m = $REQUEST$METHOD.matcher(sentence); m}.find) {
        sentence = sentence.replace(m.group(0), "").trim()
        (m.group(1).toUpperCase, m.group(3))
    }
    else {
        ("GET", """""""")
    }

    val URL: String = if ($BLANK.test(sentence)) {
        Output.writeWarning("Unrecognized REQUEST phrase " + sentence.takeAfter($BLANK))
        sentence.takeBefore($BLANK)
    }
    else {
        sentence
    }

    def execute(PQL: PQL): Unit = {
        val url = this.URL.$eval(PQL).asText
        val data = this.data.$eval(PQL).asText
        val http: Http =
            this.method match {
                case "POST" => Http.POST(url, data)
                case "PUT" => Http.PUT(url, data)
                case "DELETE" => Http.DELETE(url, data)
                case _ => Http.GET(url)
            }
        if (this.header.nonEmpty) {
            for ((k, v) <- this.header) {
                http.setHeader(k, v)
            }
        }

        PQL.dh.openJson(http.request())
    }
}