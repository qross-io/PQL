package io.qross.net

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.springframework.web.context.request.{RequestContextHolder, ServletRequestAttributes}

object HttpServlet {
    def request: HttpServletRequest = {
        val attributes: ServletRequestAttributes = RequestContextHolder.getRequestAttributes.asInstanceOf[ServletRequestAttributes]
        if (attributes != null) {
            attributes.getRequest
        }
        else {
            null
        }
    }

    def response: HttpServletResponse = {
        val attributes: ServletRequestAttributes = RequestContextHolder.getRequestAttributes.asInstanceOf[ServletRequestAttributes]
        if (attributes != null) {
            attributes.getResponse
        }
        else {
            null
        }
    }
}
