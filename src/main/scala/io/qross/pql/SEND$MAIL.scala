package io.qross.pql

import io.qross.pql.Patterns._

class SEND$MAIL(info: String) {

    //SET CONTENT
    //FROM TEMPLATE
    //FROM DEFAULT TEMPLATE
    //WITH DEFAULT SIGNATURE
    //WITH SIGNATURE
    //ATTACH
    //TO
    //CC
    //BCC

    private val values = info.split($LINK.regex, -1).map(_.trim())
    private val links = $LINK.findAllIn(info).map(m => m.trim().replaceAll("""\s+""", "\\$").toUpperCase()).toArray

    def send(): Unit = {

        val title = values(0)

        for (i <- links.indices) {

        }
    }
}
