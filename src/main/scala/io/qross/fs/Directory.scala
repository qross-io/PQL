package io.qross.fs

import java.io.{File, FileFilter}

import io.qross.ext.TypeExt._

import scala.util.matching.Regex

object Directory {

    def listFiles(path: String): Array[File] = {
        val dir = new File(path)
        if (dir.exists()) {
            if (dir.isDirectory) {
                dir.listFiles(
                    new FileFilter() {
                        override def accept(f: File): Boolean = f.isFile
                    })
            }
            else {
                Array[File](dir)
            }
        }

        else {
            new Array[File](0)
        }
    }

    def listFiles(path: String, filter: String): Array[File] = {
        listFiles(path, filter.replace("?", "[\\s\\S]")
                            .replace("[\\s\\S]*", "")
                            .replace(".", "\\.")
                            .r)
    }

    def listFiles(path: String, filter: Regex): Array[File] = {
        val dir = new File(path)
        if (dir.exists()) {
            if (dir.isDirectory) {
                dir.listFiles(
                    new FileFilter {
                        override def accept(f: File): Boolean = {
                            filter.test(f.getPath)
                        }
                    }
                )
            }
            else if (filter.test(path)) {
                Array[File](dir)
            }
            else {
                new Array[File](0)
            }
        }
        else {
            new Array[File](0)
        }
    }

    def spaceUsage(path: String): Long = {
        var size = 0L
        val dir = new File(path)
        if (dir.isDirectory) {
            for (file <- dir.listFiles()) {
                if (file.isDirectory) {
                    size += spaceUsage(file.getPath)
                }
                else {
                    size += file.length()
                }
            }
        }
        else {
            size += dir.length()
        }

        size
    }

    def exists(path: String): Boolean = {
        val dir = new File(path)
        dir.exists && dir.isDirectory
    }

    def create(path: String): Boolean = {
        val dir = new File(path)
        if (!dir.exists) {
            dir.mkdirs
        }
        else {
            false
        }
    }

    def clear(path: String): Unit = {
        val dir = new File(path)
        if (dir.isDirectory) {
            for (f <- dir.listFiles) {
                if (f.isDirectory) {
                    clear(f.getPath)
                }
                else {
                    f.delete()
                }
            }
        }
        else {
            dir.delete
        }
    }

    def delete(path: String): Boolean = {
        clear(path)
        val dir = new File(path)
        dir.delete()
    }
}
