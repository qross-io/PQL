package io.qross.pql

import io.qross.pql.Patterns.$PHRASE
import io.qross.ext.TypeExt._

import scala.collection.mutable.ArrayBuffer

object Phrase {
    def parse(sentence: String, links: Set[String]): ArrayBuffer[(String, String)] = {

        var exp = sentence
        val sections = new ArrayBuffer[(String, String)]

        // TABLE, PRIMARY KEY, UNIQUE KEY, KEY

        // TABLE table PRIMARY   key id UNIQUE KEY (c1, c2) KEY (c3);

        // TABLE -> table, PRIMARY KEY -> id, UNIQUE KEY -> (c1, c2), KEY -> (c3)

        $PHRASE.findAllIn(exp).foreach(phrase => {

//            var phr = phrase
//
//            do {
//                var current = phr.trim().replaceAll("\\s+", "\\$")
//            }
//            while(true)
//
//
//
//            while (!links.contains(phrase) && phrase.contains("$")) {
//                phrase = phrase.takeBeforeLast("$")
//            }
//
//            if (phrase != "") {
//
//            }
//            else {
//                throw new SQLParseException("Unrecognized or wrong phrase: " + phr)
//            }
//
//            while (phrase != "") {
//
//                while (!links.contains(phrase)) {
//                    phrase = phrase.takeBeforeLast("$")
//                }
//                sections += ((phrase, ""))
//            }
        })

        sections
    }
}

//phrase = "", 表示仅仅是参数
class Phrase(words: String, args: String) { }