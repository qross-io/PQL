package io.qross.fs

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.collection.mutable

object Zip {

    def main(args: Array[String]): Unit = {
        // files = List(new File("D:\\data\\platform\\logs\\platform.log"),
        //    new File("D:\\data\\platform\\logs\\platform2018-10-15.log"),
        //    new File("D:\\data\\platform\\logs\\platform2018-10-22.log"))
        //compress(files, "d:/test.zip")
    }

    /*
    public static void toZip(String srcDir, OutputStream out, boolean KeepDirStructure)
    31
    throws RuntimeException{
        32

        33
        long start = System.currentTimeMillis();
        34
        ZipOutputStream zos = null ;
        35
        try {
            36
            zos = new ZipOutputStream(out);
            37
            File sourceFile = new File(srcDir);
            38
            compress(sourceFile,zos,sourceFile.getName(),KeepDirStructure);
            39
            long end = System.currentTimeMillis();
            40
            System.out.println("压缩完成，耗时：" + (end - start) +" ms");
            41
        } catch (Exception e) {
            42
            throw new RuntimeException("zip error from ZipUtils",e);
            43
        }finally{
            44
            if(zos != null){
                45
                try {
                    46
                    zos.close();
                    47
                } catch (IOException e) {
                    48
                    e.printStackTrace();
                    49
                }
                50
            }
            51
        }
        52

        53
    }
    54

    55
    /**
    56
      * 压缩成ZIP 方法2
57
      * @param srcFiles 需要压缩的文件列表
58
      * @param out           压缩文件输出流
59
      * @throws RuntimeException 压缩失败会抛出运行时异常
60
      */
    61
    public static void toZip(List<File> srcFiles , OutputStream out)throws RuntimeException {
        62
        long start = System.currentTimeMillis();
        63
        ZipOutputStream zos = null ;
        64
        try {
            65
            zos = new ZipOutputStream(out);
            66
            for (File srcFile : srcFiles) {
                67
                byte[] buf = new byte[BUFFER_SIZE];
                68
                zos.putNextEntry(new ZipEntry(srcFile.getName()));
                69
                int len;
                70
                FileInputStream in = new FileInputStream(srcFile);
                71
                while ((len = in.read(buf)) != -1){
                    72
                    zos.write(buf, 0, len);
                    73
                }
                74
                zos.closeEntry();
                75
                in.close();
                76
            }
            77
            long end = System.currentTimeMillis();
            78
            System.out.println("压缩完成，耗时：" + (end - start) +" ms");
            79
        } catch (Exception e) {
            80
            throw new RuntimeException("zip error from ZipUtils",e);
            81
        }finally{
            82
            if(zos != null){
                83
                try {
                    84
                    zos.close();
                    85
                } catch (IOException e) {
                    86
                    e.printStackTrace();
                    87
                }
                88
            }
            89
        }
        90
    }
    91

    92

    93
    /**
    94
      * 递归压缩方法
95
      * @param sourceFile 源文件
96
      * @param zos        zip输出流
97
      * @param name       压缩后的名称
98
      * @param KeepDirStructure  是否保留原来的目录结构,true:保留目录结构;
99
      *                          false:所有文件跑到压缩包根目录下(注意：不保留目录结构可能会出现同名文件,会压缩失败)
100
      * @throws Exception
    101
      */
    102
    private static void compress(File sourceFile, ZipOutputStream zos, String name,
        103
            boolean KeepDirStructure) throws Exception{
        104
        byte[] buf = new byte[BUFFER_SIZE];
        105
        if(sourceFile.isFile()){
            106
            // 向zip输出流中添加一个zip实体，构造器中name为zip实体的文件的名字
            107
            zos.putNextEntry(new ZipEntry(name));
            108
            // copy文件到zip输出流中
            109
            int len;
            110
            FileInputStream in = new FileInputStream(sourceFile);
            111
            while ((len = in.read(buf)) != -1){
                112
                zos.write(buf, 0, len);
                113
            }
            114
            // Complete the entry
            115
            zos.closeEntry();
            116
            in.close();
            117
        } else {
            118
            File[] listFiles = sourceFile.listFiles();
            119
            if(listFiles == null || listFiles.length == 0){
                120
                // 需要保留原来的文件结构时,需要对空文件夹进行处理
                121
                if(KeepDirStructure){
                    122
                    // 空文件夹的处理
                    123
                    zos.putNextEntry(new ZipEntry(name + "/"));
                    124
                    // 没有文件，不需要文件的copy
                    125
                    zos.closeEntry();
                    126
                }
                127

                128
            }else {
                129
                for (File file : listFiles) {
                    130
                    // 判断是否需要保留原来的文件结构
                    131
                    if (KeepDirStructure) {
                        132
                        // 注意：file.getName()前面需要带上父文件夹的名字加一斜杠,
                        133
                        // 不然最后压缩包中就不能保留原来的文件结构,即：所有文件都跑到压缩包根目录下了
                        134
                        compress(file, zos, name + "/" + file.getName(),KeepDirStructure);
                        135
                    } else {
                        136
                        compress(file, zos, file.getName(),KeepDirStructure);
                        137
                    }
                    138

                    139
                }
                140
            }
            141
        }
        142
    }
    */
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
        this.zipList += new File(path)
    }
}
