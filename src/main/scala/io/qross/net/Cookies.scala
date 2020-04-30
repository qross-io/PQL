package io.qross.net

import io.qross.core.{DataCell, DataType}
import javax.servlet.http.Cookie

import scala.util.control.Breaks._

object Cookies {

    def get(name: String): String = {
        val request = Servlet.httpRequest
        if (request != null) {
            val cookies = request.getCookies
            var value: String = null
            breakable {
                for (cookie <- cookies) {
                    if (cookie.getName == name) {
                        value = cookie.getValue
                        break
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

    def set(name: String, value: Any): Unit = {
        val response = Servlet.httpResponse
        if (response != null) {
            response.addCookie(new Cookie(name, value.toString))
        }
    }

    def update(cookies: Cookie*): Unit = {
        val response = Servlet.httpResponse
        if (response != null) {
            cookies.foreach(response.addCookie)
        }
    }
}

class Cookies {
    def getCell(name: String): DataCell = {
        val value = Cookies.get(name)
        if (value == null) {
            DataCell(value, DataType.TEXT)
        }
        else {
            DataCell.NOT_FOUND
        }
    }

    override def toString: String = "io.qross.net.Cookies"
}
