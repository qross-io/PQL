package cn.qross.fs

object CodeCounter {
    def count(path: String, filter: String): Int = {
        Directory.listFiles(path, filter, recursive = true).map(file => new FileReader(file.getPath).countLines).sum
    }

    def count(path: String): Int = {
        Directory.listFiles(path, recursive = true).map(file => new FileReader(file.getPath).countLines).sum
    }

    def main(args: Array[String]): Unit = {

        val path = "c:/cn.qross/"

        Map[String, Int](
            "PQL" -> count(path + "PQL/src/main/scala/"),
                "Keeper" -> count(path + "Keeper/src/main/scala/"),
                "Master" -> count(path + "Master/src/main/"),
                "Worker" -> count(path + "Worker/src/main/"),
                "root.js" -> count(path + "root.js/", "root*.js")
        ).foreach(k => {
            println(k._1 + ": " + k._2)
        })
    }
}