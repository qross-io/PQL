package io.qross.ext

//处理Scala中的类型擦除

class Erasure {

}

case class ParameterMap(queries: java.util.Map[String, Array[String]])

case class ArgumentMap(args: Map[String, String])

case class ScalaList(list: List[Any])

case class ScalaMap(row: Map[String, Any])

case class JavaList(list: java.util.List[Any])

case class JavaMap(map: java.util.Map[String, Any])