package io.qross.fs

object CodeCounter {
    def count(path: String, filter: String): Int = {
        Directory.listFiles(path, filter, recursive = true).map(file => FileReader(file).countLines).sum
    }

    def count(path: String): Int = {
        Directory.listFiles(path, recursive = true).map(file => FileReader(file).countLines).sum
    }

    def main(args: Array[String]): Unit = {

        val path = "c:/io.qross/"
        //val path = "f:/"

        Map[String, Int](
            "PQL" -> count(path + "PQL/src/main/scala/"),
                "Keeper" -> count(path + "Keeper/src/main/scala/"),
                "Master" -> count(path + "Master/src/main/"),
                "Worker" -> count(path + "Worker/src/main/")
        ).foreach(k => {
            println(k._1 + ": " + k._2)
        })
    }
}