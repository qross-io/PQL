package io.qross.exception

class FieldNotFoundException(val s: String) extends RuntimeException(s) {

}

class ConvertFailureException(val s: String) extends RuntimeException(s) {

}

class ExtensionNotFoundException(val s: String) extends RuntimeException(s) {

}

class JsonParseException(val s: String) extends RuntimeException(s) {

}

class OneApiIncorrectServiceNameException(val s: String) extends RuntimeException(s) {

}