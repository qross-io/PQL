package io.qross.sql

import io.qross.ext.TypeExt._
import io.qross.setting.Properties

/*
OPEN connectionName;
OPEN connectionName USE databaseName;
OPEN CACHE;
OPEN TEMP;
OPEN DEFAULT;
*/

class OPEN(val sections: String*) {

    var sourceType: String = ""
    var connectionName: String = ""
    var databaseName: String = ""

    sections(0).toUpperCase() match {
        case "CACHE" => sourceType = "CACHE"
        case "TEMP" => sourceType = "TEMP"
        case "DEFAULT" => sourceType = "DEFAULT"
        case _ =>
            connectionName = sections(0)
            if (sections.length > 2) {
                if (sections(1).equalsIgnoreCase("USE")) {
                    databaseName = sections(2)
                }
            }
    }

    /*
    sourceType = sourceType.$replace("""\s\s""", " ")
    if (use == null) {
        use = ""
    }

    if (source.matches("(?i)^(CACHE|TEMP|DEFAULT)$")) {
        source = source.toUpperCase()
    }
    else {
        if (!Properties.contains(source)) {
            throw new SQLParseException("Wrong connection name: " + source)
        }
    }
    */
}
