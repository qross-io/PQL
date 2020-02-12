package io.qross.core

class FieldNotFoundException(val s: String) extends RuntimeException(s) {

}

class ConvertFailureException(val s: String) extends RuntimeException(s) {

}

class ExtensionNotFoundException(val s: String) extends RuntimeException(s) {

}

class JsonParseException(val s: String) extends RuntimeException(s) {

}