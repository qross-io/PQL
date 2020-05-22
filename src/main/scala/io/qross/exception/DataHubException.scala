package io.qross.exception

class DataHubException {

}

class OpenDataSourceException(val s: String) extends RuntimeException(s) {

}

class WrongSourceNameException(val s: String) extends RuntimeException(s) {

}

class IncorrectDataSourceException(val s: String) extends RuntimeException(s) {

}

class DefineAliasException(val s: String) extends RuntimeException(s) {

}

