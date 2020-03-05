package io.qross.app;

import io.qross.core.DataHub;
import io.qross.fs.ResourceFile;
import io.qross.pql.PQL;
import org.springframework.web.servlet.view.AbstractTemplateView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Voyager extends AbstractTemplateView {

    @Override
    protected void renderMergedTemplateModel(Map<String, Object> model, HttpServletRequest request, HttpServletResponse response) throws Exception {

        String url = getUrl();
        String name = getBeanName();
        String dir;
        if (name == null) {
            dir = url.substring(0, url.substring(1).indexOf("/") + 2);
        }
        else {
            dir = url.substring(0, url.indexOf(name));
        }

        String content = ResourceFile.open(url).content();

        //替换server includes
        Pattern p = Pattern.compile("<#\\s*include\\s+[\"'](.+?)[\"']\\s*/>", Pattern.CASE_INSENSITIVE);
        Matcher m;
        while ((m = p.matcher(content)).find()) {
            String path = dir + m.group(1);
            path = path.replace("//", "/");
            content = content.replace(m.group(0), ResourceFile.open(path).content());
        }

        Map<String, Object> attributes = getAttributesMap();

        response.setCharacterEncoding(attributes.get("charset").toString());
        response
                .getWriter()
                .write(
                        new PQL("EMBEDDED:" + content, new DataHub(attributes.get("connection").toString()))
                                .placeParameters(request.getParameterMap())
                                .set(model)
                                .run()
                                .toString()
                );
    }
}