package io.qross.ext

object ToScala {
    def ArrayToSeq(javaArray: Array[Any]): Seq[Any] = {
        javaArray.toSeq
    }
}
