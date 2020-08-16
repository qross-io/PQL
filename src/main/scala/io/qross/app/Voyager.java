package io.qross.app;

import io.qross.core.DataHub;
import io.qross.ext.Console;
import io.qross.fs.ResourceFile;
import io.qross.net.Cookies;
import io.qross.net.HttpRequest;
import io.qross.pql.PQL;
import io.qross.setting.Configurations;
import io.qross.setting.Language;
import io.qross.setting.Properties;
import org.springframework.web.servlet.view.AbstractTemplateView;
import scala.collection.immutable.Stream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class  Voyager extends AbstractTemplateView {

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

            //替换server includes
            Pattern p = Pattern.compile("<#\\s*include\\s+file=[\"'](.+?)[\"']\\s*/>", Pattern.CASE_INSENSITIVE);
            Matcher m;
            //include file - can nest
            while ((m = p.matcher(content)).find()) {
                String path = dir + m.group(1);
                path = path.replace("//", "/");
                if (path.endsWith(".sql")) {
                    content = content.replace(m.group(0), "<%" + ResourceFile.open(path).content() + "%>");
                }
                else {
                    content = content.replace(m.group(0), ResourceFile.open(path).content());
                }
            }

            //Map<String, Object> attributes = getAttributesMap();
            response.setCharacterEncoding(Setting.VoyagerCharset);

            HttpRequest http = new HttpRequest(request);

            Object result =
                    new PQL("EMBEDDED:" + content, new DataHub(Properties.contains(Setting.VoyagerConnection) ? Setting.VoyagerConnection : ""))
                        .place(http.getParameters())
                        .set("request", http.getRequestInfo())
                        .set(model)
                        .run();
            response
                    .getWriter()
                    .write(result == null ? "" : result.toString());
        }
    }
}