package io.qross.sql

import java.util.regex.Matcher

import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.sql.Patterns._

class REQUEST(var sentence: String) {

    private var m: Matcher = _

    val header: Map[String, String] = if ({ m = $REQUEST$HEADER.matcher(sentence); m}.find) {
        sentence = sentence.replace(m.group(0), "").trim()
        m.group(1).toHashMap()
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
}