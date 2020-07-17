package io.qross.ext

object ToScala {
    def ArrayToSeq(javaArray: Array[Any]): Seq[Any] = {
        javaArray.toSeq
    }

    def EmptyArgs: Seq[Any] = {
        Seq[Any]()
    }
}
