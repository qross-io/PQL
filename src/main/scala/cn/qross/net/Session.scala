package cn.qross.net

import cn.qross.core.{DataCell, DataType}
import javax.servlet.http.Cookie
import org.springframework.web.context.request.{RequestContextHolder, ServletRequestAttributes}

object Session {

    def get(name: String): Any = {
        val request = HttpServlet.request
        if (request != null) {
            request.getSession.getAttribute(name)
        }
        else {
            null
        }
    }

    def set(name: String, value: Any): Unit = {
        val request = HttpServlet.request
        if (request != null) {
            request.getSession.setAttribute(name, value)
        }
    }
}

class Session {
    def getCell(name: String): DataCell = {
        val value = Session.get(name)
        if (value != null) {
            DataCell(value)
        }
        else {
            DataCell.UNDEFINED
        }
    }

    override def toString: String = "cn.qross.net.Session"
}
