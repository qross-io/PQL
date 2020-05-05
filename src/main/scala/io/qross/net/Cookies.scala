package io.qross.net

import io.qross.core.{DataCell, DataRow, DataType}
import javax.servlet.http.Cookie

import scala.util.control.Breaks._

object Cookies {

    def get(name: String): String = {
        val request = Servlet.httpRequest
        if (request != null) {
            val cookies = request.getCookies
            var value: String = null
            if (cookies != null) {
                breakable {
                    for (cookie <- cookies) {
                        if (cookie.getName == name) {
                            value = cookie.getValue
                            break
                        }
                    }
                }
            }
            value
        }
        else {
            null
        }
    }

    def getOrElse(name: String, defaultValue: String): String = {
        val value = get(name)
        if (value == null) {
            defaultValue
        }
        else {
            value
        }
    }

    def set(name: String, value: String): Unit = {
        val response = Servlet.httpResponse
        if (response != null) {
            val cookie = new Cookie(name, value)
            cookie.setPath("/")
            cookie.setMaxAge(3600 * 24 * 30)  //默认保存30天
            response.addCookie(cookie)
        }
    }
}

class Cookies {

    val editing = new DataRow()

    def getCell(name: String): DataCell = {
        val value = Cookies.get(name)
        if (value != null) {
            DataCell(value, DataType.TEXT)
        }
        else {
            DataCell.UNDEFINED
        }
    }

    def setValue(pairs: (String, String)*): Cookies = {
        pairs.foreach(pair => {
            editing.set(pair._1, pair._2, DataType.TEXT)
            Cookies.set(pair._1, pair._2)
        })
        this
    }

    def setValue(map: DataRow): Cookies = {
        map.foreach((field, value) => {
            Cookies.set(field, value.toString)
        })
        editing.combine(map)
        this
    }

    //-1 直到浏览器关闭  0 expire >0 seconds
    def setMaxAge(seconds: Int): Cookies = {
        if (editing.nonEmpty) {
            val request = Servlet.httpRequest
            val response = Servlet.httpResponse
            if (request != null && response != null) {
                val cookies = request.getCookies
                for (cookie <- cookies) {
                    if (editing.contains(cookie.getName)) {
                        cookie.setMaxAge(seconds)
                        response.addCookie(cookie)
                    }
                }
            }
        }
        this
    }

    def setDomain(domain: String): Cookies = {
        if (editing.nonEmpty) {
            val request = Servlet.httpRequest
            val response = Servlet.httpResponse
            if (request != null && response != null) {
                val cookies = request.getCookies
                for (cookie <- cookies) {
                    if (editing.contains(cookie.getName)) {
                        cookie.setDomain(domain)
                        response.addCookie(cookie)
                    }
                }
            }
        }
        this
    }

    def setPath(path: String): Cookies = {
        if (editing.nonEmpty) {
            val request = Servlet.httpRequest
            val response = Servlet.httpResponse
            if (request != null && response != null) {
                val cookies = request.getCookies
                for (cookie <- cookies) {
                    if (editing.contains(cookie.getName)) {
                        cookie.setPath(path)
                        response.addCookie(cookie)
                    }
                }
            }
        }
        this
    }

    def setHttpOnly(httpOnly: Boolean): Cookies = {
        if (editing.nonEmpty) {
            val request = Servlet.httpRequest
            val response = Servlet.httpResponse
            if (request != null && response != null) {
                val cookies = request.getCookies
                for (cookie <- cookies) {
                    if (editing.contains(cookie.getName)) {
                        cookie.setHttpOnly(httpOnly)
                        response.addCookie(cookie)
                    }
                }
            }
        }
        this
    }

    def setSecure(secure: Boolean): Cookies = {
        if (editing.nonEmpty) {
            val request = Servlet.httpRequest
            val response = Servlet.httpResponse
            if (request != null && response != null) {
                val cookies = request.getCookies
                for (cookie <- cookies) {
                    if (editing.contains(cookie.getName)) {
                        cookie.setHttpOnly(secure)
                        response.addCookie(cookie)
                    }
                }
            }
        }
        this
    }

    def setVersion(version: Int): Cookies = {
        if (editing.nonEmpty) {
            val request = Servlet.httpRequest
            val response = Servlet.httpResponse
            if (request != null && response != null) {
                val cookies = request.getCookies
                for (cookie <- cookies) {
                    if (editing.contains(cookie.getName)) {
                        cookie.setVersion(version)
                        response.addCookie(cookie)
                    }
                }
            }
        }
        this
    }

    def setComment(comment: String): Cookies = {
        if (editing.nonEmpty) {
            val request = Servlet.httpRequest
            val response = Servlet.httpResponse
            if (request != null && response != null) {
                val cookies = request.getCookies
                for (cookie <- cookies) {
                    if (editing.contains(cookie.getName)) {
                        cookie.setComment(comment)
                        response.addCookie(cookie)
                    }
                }
            }
        }
        this
    }



    //删除
    def remove(name: String*): Cookies = {
        val list = name.toSet
        val request = Servlet.httpRequest
        val response = Servlet.httpResponse
        if (request != null && response != null) {
            val cookies = request.getCookies
            for (cookie <- cookies) {
                if (list.contains(cookie.getName)) {
                    cookie.setMaxAge(0)
                    response.addCookie(cookie)
                }
            }
        }

        this
    }

    override def toString: String = "io.qross.net.Cookies"
}
