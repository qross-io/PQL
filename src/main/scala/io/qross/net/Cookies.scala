package io.qross.net

import io.qross.core.{DataRow, DataTable}
import javax.servlet.http.{Cookie, HttpServletRequest, HttpServletResponse}

class Cookies(val request: HttpServletRequest, val response: HttpServletResponse) {

    private lazy val cookies = new DataTable()

    if (request != null) {
        request.getCookies.foreach(cookie => {
            cookies.insert(
                    "name" -> cookie.getName,
                    "value" -> cookie.getValue,
                    "domain" -> cookie.getDomain,
                    "path" -> cookie.getPath,
                    "max_age" -> cookie.getMaxAge, //seconds
                    "secure" -> cookie.getSecure,
                    "comment" -> cookie.getComment,
                    "version" -> cookie.getVersion
            )
        })
    }

    def this(request: HttpServletRequest) {
        this(request, null)
    }

    def this(response: HttpServletResponse) {
        this(null, response)
    }

    def get(name: String): String = {
        ""
    }
}
