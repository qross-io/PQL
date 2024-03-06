package cn.qross.fs

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import cn.qross.core.DataHub
import cn.qross.fs.Path._
import cn.qross.net.Email

import scala.collection.mutable

object Zip {

    implicit class DataHub$Zip(val dh: DataHub) {

        def ZIP: Zip = {
            if (!dh.slots("ZIP")) {
                dh.plug("ZIP", new Zip())
            }
            dh.pick[Zip]("ZIP").orNull
        }

        def andZip(fileNameOrFullPath: String): DataHub = {
            ZIP.compress(fileNameOrFullPath.locate())
            dh
        }

        def andZipAll(fileNameOrFullPath: String): DataHub = {
            ZIP.compressAll(fileNameOrFullPath.locate())
            dh
        }

        def addFileToZipList(fileNameOrFullPath: String): DataHub = {
            ZIP.addFile(fileNameOrFullPath.locate())
            dh
        }

        //清除列表并删除文件
        def andClear(): DataHub = {
            ZIP.deleteAll()
            dh
        }

        //仅清除列表
        def resetZipList(): DataHub = {
            ZIP.clear()
            dh
        }

        def showZipList(): DataHub = {
            ZIP.zipList.foreach(file => println(file.getAbsolutePath))
            dh
        }

        def attachZipToEmail(title: String): DataHub = {
            dh.plug("EMAIL", new Email(title))
              .pick[Email]("EMAIL").orNull
              .attach(ZIP.zipFile)
            dh
        }
    }
}

class Zip {

    val zipList = new mutable.HashSet[File]()
    var zipFile = ""

    def compress(fileName: String): Zip = {

        zipFile = fileName

        val out = new FileOutputStream(zipFile)
        val zos = new ZipOutputStream(out)

        val buf = new Array[Byte](2048)
        zos.putNextEntry(new ZipEntry(zipList.last.getName))

        val fin = new FileInputStream(zipList.last)
        var len = fin.read(buf)
        while (len != -1) {
            zos.write(buf, 0, len)
            len = fin.read(buf)
        }
        zos.closeEntry()
        fin.close()


        zos.close()
        this
    }

    //文件列表
    def compressAll(fileName: String): Zip = {

        zipFile = fileName

        val out = new FileOutputStream(zipFile)
        val zos = new ZipOutputStream(out)

        for (file <- zipList) {
            val buf = new Array[Byte](2048)
            zos.putNextEntry(new ZipEntry(file.getName))

            val fin = new FileInputStream(file)
            var len = fin.read(buf)
            while (len != -1) {
                zos.write(buf, 0, len)
                len = fin.read(buf)
            }
            zos.closeEntry()
            fin.close()
        }

        zos.close()

        this
    }

    def deleteAll(): Unit = {
        zipList.foreach(file => file.delete())
        zipList.clear()
    }

    def clear(): Unit = {
        zipList.clear()
    }

    def addFile(path: String): Unit = {
        this.zipList += new File(path.locate())
    }
}
