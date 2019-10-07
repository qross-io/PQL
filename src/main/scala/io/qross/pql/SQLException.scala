package io.qross.pql

class SQLParseException(val s: String) extends RuntimeException(s) {

}

class SQLExecuteException(val s: String) extends RuntimeException(s) {

}

class SharpLinkArgumentException(val s: String) extends RuntimeException(s) {

}