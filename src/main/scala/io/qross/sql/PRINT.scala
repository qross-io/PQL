package io.qross.sql

object PRINT {
    val WARN: String = "WARN"
    val ERROR: String = "ERROR"
    val DEBUG: String = "DEBUG"
    val INFO: String = "INFO"
    val NONE: String = "NONE"
}

class PRINT(var messageType: String, val message: String) {
    if (messageType == null) {
        messageType = "NONE"
    }
    else {
        messageType = messageType.trim.toUpperCase()
    }
}
