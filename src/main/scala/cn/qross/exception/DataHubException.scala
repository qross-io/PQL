package cn.qross.exception

class DataHubException {

}

class OpenDataSourceException(val s: String) extends RuntimeException(s) {

}

class NoDataSourceException(val s: String) extends RuntimeException(s) {

}

class NoDataDestinationException(val s: String) extends RuntimeException(s) {

}

class WrongSourceNameException(val s: String) extends RuntimeException(s) {

}

class IncorrectDataSourceException(val s: String) extends RuntimeException(s) {

}

class DefineAliasException(val s: String) extends RuntimeException(s) {

}

class ColumnNotFoundException(val s: String) extends RuntimeException(s) {

}

