package io.qross.security;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.qross.ext.Console;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.HashMap;
import java.util.Map;

public class Cookies {

    private static Map<String, String> cookies = new HashMap<>();

    public Cookies() {
        loadCookies();
    }

    private void loadCookies() {

        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        HttpServletRequest request = attributes.getRequest();

        Cookie[] cs = request.getCookies();
        for (int i = 0; i < (cs == null ? 0 : cs.length); i++) {
            cookies.put(cs[i].getName(), cs[i].getValue());
        }
    }

    public static Cookies load() {
        return new Cookies();
    }

    public String get(String name) {
        return cookies.getOrDefault(name, "");
    }

    public static String take(String name) {
        return Cookies.load().get(name);
    }

    public static void main(String[] args) {
        Console.writeLine((ServletRequestAttributes) RequestContextHolder.getRequestAttributes());
        //ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        //HttpServletRequest request = attributes.getRequest();
    }

    public static void test() {
        Console.writeLine((ServletRequestAttributes) RequestContextHolder.getRequestAttributes());
    }

    /*
    public static void saveCookie(Cookie cookie) {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        HttpServletResponse response = attributes.getResponse();
        response.addCookie(cookie);
    }

    public static void addCookie(String name, Object object) {
        try {

            String v = URLEncoder.encode(new Gson().toJson(object), "UTF-8");

            Cookie cookie = new Cookie(name, v);
            cookie.setPath("/");
            cookie.setMaxAge(Integer.MAX_VALUE);
            saveCookie(cookie);

        } catch (Exception e) {
            System.out.println(" -------add cookie--------" + e.getMessage());
        }
    }

    public static String getCookie(String name) {
        try {

            Cookie[] cookies = getCookies();

            for (int i = 0; i < (cookies == null ? 0 : cookies.length); i++) {
                if ((name).equalsIgnoreCase(cookies[i].getName())) {
                    return URLDecoder.decode(cookies[i].getValue(), "UTF-8");
                }
            }
        } catch (Exception e) {
            System.out.println(" --------String cookie-------   " + e.getMessage());
        }
        return null;
    }

    public static void removeCookie(String name) {
        try {
            Cookie[] cookies = getCookies();
            for (int i = 0; i < (cookies == null ? 0 : cookies.length); i++) {
                if ((name).equalsIgnoreCase(cookies[i].getName())) {

                    Cookie cookie = new Cookie(name, "");
                    cookie.setPath("/");
                    cookie.setMaxAge(0);// expire
                    saveCookie(cookie);
                }
            }
        } catch (Exception e) {

        }
    }
    */
}
