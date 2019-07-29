package io.qross.sql

import io.qross.setting.Properties
import io.qross.ext.TypeExt._

/*
SAVE AS connectionName;
SAVE AS connectionName USE databaseName;
SAVE AS DEFAULT;
SAVE AS CACHE;
SAVE AS CACHE TABLE tableName;
SAVE AS TEMP;
SAVE AS TEMP TABLE tableName;
*/

class SAVE$AS(sections: String*) {

    var targetType: String = ""
    var targetName: String = ""
    var databaseName: String = ""

    sections(0).toUpperCase() match {
        case "CACHE" =>
            if (sections.length == 1) {
                targetType = "CACHE"
            }
            else {
                if (sections(1).equalsIgnoreCase("TABLE")) {
                    targetType = "CACHE TABLE"
                    if (sections.length > 2) {
                        targetName = sections(2)
                    }
                    else {
                        throw new SQLParseException("Empty CACHE TABLE name at SAVE AS.")
                    }
                }
                else {
                    throw new SQLParseException("Wrong CACHE type: " + sections(1))
                }
            }
        case "TEMP" =>
            if (sections.length == 1) {
                targetType = "TEMP"
            }
            else {
                if (sections(1).equalsIgnoreCase("TABLE")) {
                    targetType = "TEMP TABLE"
                    if (sections.length > 2) {
                        targetName = sections(2)
                    }
                    else {
                        throw new SQLParseException("Empty TEMP TABLE name at SAVE AS.")
                    }
                }
                else {
                    throw new SQLParseException("Wrong TEMP type: " + sections(1))
                }
            }
        case _ =>
            targetType = "JDBC"
            if (sections(0).equalsIgnoreCase("DEFAULT")) {
                targetName = "DEFAULT"
            }
            else {
                targetName = sections(0)
                if (!Properties.contains(targetName)) {
                    throw new SQLParseException("Wrong connection name: " + targetName)
                }
            }

            if (sections.length > 2 && sections(1).equalsIgnoreCase("USE")) {
                databaseName = sections(2)
            }
    }
}
