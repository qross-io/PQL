package io.qross.fs

object CodeCounter {
    def count(path: String, filter: String): Int = {
        Directory.listFiles(path, filter, recursive = true).map(file => FileReader(file).countLines).sum
    }

    def count(path: String): Int = {
        Directory.listFiles(path, recursive = true).map(file => FileReader(file).countLines).sum
    }

    def main(args: Array[String]): Unit = {
        Map[String, Int](
            "PQL" -> count("c:/io.qross/PQL/src/main/scala/"),
                "Keeper" -> count("c:/io.qross/Keeper/src/main/scala/"),
                "Master" -> count("c:/io.qross/Master/src/main/")
        ).foreach(k => {
            println(k._1 + ": " + k._2)
        })
    }
}