package io.qross.app;

import io.qross.core.DataHub;
import io.qross.ext.Console;
import io.qross.fs.ResourceFile;
import io.qross.pql.PQL;
import io.qross.setting.Language;
import io.qross.setting.Properties;
import org.springframework.web.servlet.view.AbstractTemplateView;
import scala.collection.immutable.Stream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Voyager extends AbstractTemplateView {

    @Override
    protected void renderMergedTemplateModel(Map<String, Object> model, HttpServletRequest request, HttpServletResponse response) throws Exception {

        String url = getUrl();
        if (url != null) {
            //url my be contains //
            url = url.replace("//", "/");

            String name = getBeanName();
            String dir;
            if (name == null) {
                dir = url.substring(0, url.substring(1).indexOf("/") + 2);
            } else {
                dir = url.substring(0, url.indexOf(name));
            }

            String content = ResourceFile.open(url).content();
            String modules = "";

            //替换server includes
            Pattern p = Pattern.compile("<#\\s*include\\s+(file|language)=[\"'](.+?)[\"']\\s*/>", Pattern.CASE_INSENSITIVE);
            Matcher m = p.matcher(content);
            while (m.find()) {
                if (m.group(1).equalsIgnoreCase("language")) {
                    modules = m.group(2).toLowerCase();
                    content = content.replace(m.group(1), "");
                }
                else {
                    String path = dir + m.group(2);
                    path = path.replace("//", "/");
                    if (path.endsWith(".sql")) {
                        content = content.replace(m.group(0), "<%" + ResourceFile.open(path).content() + "%>");
                    }
                    else {
                        content = content.replace(m.group(0), ResourceFile.open(path).content());
                    }
                }
            }

            //替换语言
            p = Pattern.compile("#\\s*([a-z][a-z0-9]+(\\.[a-z0-9]+)*)\\s*#", Pattern.CASE_INSENSITIVE);
            m = p.matcher(content);
            while (m.find()) {
                content = content.replace(m.group(0), Language.get("", modules, m.group(1)));
            }

            Map<String, Object> attributes = getAttributesMap();

            response.setCharacterEncoding(attributes.get("charset").toString());

            Object result =
                    new PQL("EMBEDDED:" + content, new DataHub(attributes.get("connection").toString()))
                        .placeParameters(request.getParameterMap())
                        .setHttpRequest(request)
                        .set(model)
                        .run();
            response
                    .getWriter()
                    .write(result == null ? "" : result.toString());
        }
    }
}