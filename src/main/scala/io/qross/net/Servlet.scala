package io.qross.net

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.springframework.web.context.request.{RequestContextHolder, ServletRequestAttributes}

object Servlet {
    def httpRequest: HttpServletRequest = {
        val attributes: ServletRequestAttributes = RequestContextHolder.getRequestAttributes.asInstanceOf[ServletRequestAttributes]
        if (attributes != null) {
            attributes.getRequest
        }
        else {
            null
        }
    }

    def httpResponse: HttpServletResponse = {
        val attributes: ServletRequestAttributes = RequestContextHolder.getRequestAttributes.asInstanceOf[ServletRequestAttributes]
        if (attributes != null) {
            attributes.getResponse
        }
        else {
            null
        }
    }
}
