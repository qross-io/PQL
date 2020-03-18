package io.qross.net

import java.io._
import java.net.{HttpURLConnection, URL}

import io.qross.core.{DataHub, ExtensionNotFoundException}
import io.qross.fs.Path._
import org.apache.commons.codec.binary.Base64

object Http {
    def GET(url: String): Http = {
        new Http("GET", url)
    }

    def POST(url: String, data: String = ""): Http = {
        new Http("POST", url, data)
    }

    def PUT(url: String, data: String = ""): Http = {
        new Http("PUT", url, data)
    }

    def DELETE(url: String, data: String = ""): Http = {
        new Http("DELETE", url, data)
    }

    implicit class DataHub$Http(val dh: DataHub) {

        def HTTP: Http = {
            dh.pick[Http]("HTTP") match {
                case Some(http) => http
                case None => throw new ExtensionNotFoundException("Must use GET/POST/PUT/DELETE method to open a http request.")
            }
        }

        def GET(url: String): DataHub = {
            dh.plug("HTTP", Http.GET(url))
        }

        def POST(url: String, data: String = ""): DataHub = {
            dh.plug("HTTP", Http.POST(url, data))
        }

        def PUT(url: String, data: String = ""): DataHub = {
            dh.plug("HTTP", Http.PUT(url, data))
        }

        def DELETE(url: String): DataHub = {
            dh.plug("HTTP", Http.DELETE(url))
        }

        def setHeader(name: String, value: String): DataHub = {
            HTTP.setHeader(name, value)
            dh
        }

        def toJson: DataHub = {
            dh.plug("JSON", HTTP.toJson)
        }
    }
}

class Http(var method: String, var url: String, var data: String = "") {

    private val conn: HttpURLConnection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod(method)
    conn.setDoInput(true)
    conn.setDoOutput(true)
    if (method == "POST") {
        conn.setUseCaches(false) // cant't cache when use post method
    }

    def send(data: String): Http = {
        this.data = data
        this
    }

    def setHeader(name: String, value: String): Http = {
        conn.setRequestProperty(name, value)
        this
    }

    def setMethod(method: String): Http = {
        this.method = method.toUpperCase()
        conn.setRequestMethod(this.method)
        this
    }

    //base authorization
    def authorize(username: String, password: String): Http = {
        conn.setRequestProperty("Authorization", "Basic " + new String(Base64.encodeBase64((username + ":" + password).getBytes)))
        this
    }

    //x authentication
    def xAuthenticate(username: String, password: String): Http = {
        conn.setRequestProperty("X-AUTH-USERNAME", username)
        conn.setRequestProperty("X-AUTH-PASSWORD", password)
        this
    }

    //z authentication
    def zAuthenticate(username: String, password: String): Http = {
        conn.setRequestProperty("Z-Request-User-Account", username)
        conn.setRequestProperty("Z-Request-User-Password", password)
        this
    }

    def request(): String = {

        val buffer = new StringBuilder

        conn.addRequestProperty("Content-Type", "application/json; charset=utf-8")
        conn.setRequestProperty("Connection", "Keep-Alive")
        conn.setRequestProperty("Charset", "UTF-8")

        conn.connect()

        //这句话有问题，如果传送data，第127行会执行出错
        if (this.data != "") {
            val os = conn.getOutputStream
            os.write(this.data.getBytes("utf-8"))
            os.close()
        }

        // read response content
        val reader = new BufferedReader(new InputStreamReader(conn.getInputStream, "utf-8"))
        var line: String = ""
        while ({ line = reader.readLine(); line } != null) {
            buffer.append(line)
        }
        reader.close()

        buffer.toString
    }

    def upload(path: String): Unit = {

        val file = new File(path.locate())
        if (!file.exists() || !file.isFile) {
            System.err.println("File not found!")
        }

        // border needed
        val BOUNDARY = "----------" + System.currentTimeMillis();
        conn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + BOUNDARY)
        // main body
        // first part
        val sb = new StringBuilder()
        sb.append("--"); // 必须多两道线
        sb.append(BOUNDARY)
        sb.append("\r\n")
        sb.append("Content-Disposition: form-data;name=\"file\";filename=\"" + file.getName + "\"\r\n")
        sb.append("Content-Type:application/octet-stream\r\n\r\n")
        val head = sb.toString.getBytes("utf-8")

        val out = new DataOutputStream(conn.getOutputStream)
        out.write(head)
        out.write(data.getBytes("utf-8"))
        val in = new DataInputStream(new FileInputStream(file))
        var bytes = 0
        val bufferOut = new Array[Byte](1024)
        while ({ bytes = in.read(bufferOut); bytes} != -1) {
            out.write(bufferOut, 0, bytes)
        }
        in.close()
        val foot = ("\r\n--" + BOUNDARY + "--\r\n").getBytes("utf-8")
        out.write(foot)
        out.flush()
        out.close()

        val buffer = new StringBuilder()
        val reader = new BufferedReader(new InputStreamReader(conn.getInputStream, "utf-8"))
        var line: String = ""
        while ({ line = reader.readLine(); line } != null) {
            buffer.append(line)
        }
        reader.close()

        buffer.toString
    }

    def toJson: Json = {
        new Json(request())
    }
}

/*
Http.GET(url)
    .POST(url, data = "")
    .DELETE(url)
    .PUT(url, data="")
    .send(data: String)
    .setHeader("name", "value")
    .authorize("username", "password")
    .request(method, url, data = "")
    .upload(file)
    .toJson()
*/
