package io.qross.app;

import io.qross.ext.Console;
import io.qross.setting.Global;
import org.springframework.web.servlet.view.AbstractTemplateView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

public class Voyager extends AbstractTemplateView {

    @Override
    protected void renderMergedTemplateModel(Map<String, Object> model, HttpServletRequest request, HttpServletResponse response) throws Exception {
        String url = getUrl();
        Console.writeLine(url);
        response.setCharacterEncoding(Global.CHARSET());
        response.getWriter().write("HELLO");
    }
}