package io.qross.app;

import io.qross.core.DataHub;
import io.qross.fs.ResourceFile;
import io.qross.net.HttpRequest;
import io.qross.pql.PQL;
import io.qross.setting.Properties;
import org.springframework.web.servlet.view.AbstractTemplateView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class  Voyager extends AbstractTemplateView {

    @Override
    protected void renderMergedTemplateModel(Map<String, Object> model, HttpServletRequest request, HttpServletResponse response) throws Exception {

        String url = getUrl();
        if (url != null) {
            //url maybe contains //
            url = url.replace("//", "/");

            String name = getBeanName();
            String dir;
            if (name == null) {
                dir = url.substring(0, url.substring(1).indexOf("/") + 2);
            } else {
                dir = url.substring(0, url.indexOf(name));
            }

            //html frist
            ResourceFile file = ResourceFile.open(url);
            boolean markdown = false;

            //markdown
            if (!file.exists()) {
                file = ResourceFile.open(url.replaceAll("(?i).html$", ".md"));
                if (file.exists()) {
                    markdown = true;
                }
            }

            //.htm
            if (!file.exists()) {
                file = ResourceFile.open(url.replaceAll("(?i).html$", ".htm"));
            }

            if (file.exists()) {

                String content = file.content();
                String template = "";
                List<String> baseArgs = new ArrayList<>();

                Pattern p = Pattern.compile("<#\\s*page\\s+template=[\"'](.+?)[\"']\\s*/>", Pattern.CASE_INSENSITIVE);
                Matcher m = p.matcher(content);
                if (m.find()) {
                    content = content.replace(m.group(0), "");
                    String uri = m.group(1);
                    if (uri.contains("?")) {
                        baseArgs.add(uri.substring(uri.indexOf("?") + 1));
                        uri = uri.substring(0, uri.indexOf("?"));
                    }

                    file = ResourceFile.open((dir + uri).replace("//", "/"));
                    if (file.exists()) {
                        template = file.content();
                    }
                }

                if (markdown) {
                    //markdown to html
                    content = Marker.open(content).transform().colorCodes().getContent();
                    if (template.isEmpty()) {
                        content = Cogo.template().replace("#{content}", content);
                    }
                    content = content.replace("&lt;%", "<%").replace("%&gt;", "%>");
                }

                if (!template.isEmpty()) {
                    content = template.replace("#{content}", content);
                }

                //scripts
                if (content.contains("#{scripts}")) {
                    content = content.replace("#{scripts}", Cogo.getScripts(content));
                }

                //replace server includes
                p = Pattern.compile("<#\\s*include\\s+file=[\"'](.+?)[\"']\\s*/>", Pattern.CASE_INSENSITIVE);
                //include file - can nest
                while ((m = p.matcher(content)).find()) {
                    String path = dir + m.group(1);
                    path = path.replace("//", "/");
                    if (path.contains("?")) {
                        baseArgs.add(path.substring(path.indexOf("?") + 1));
                        path = path.substring(0, path.indexOf("?"));
                    }
                    if (path.endsWith(".sql")) {
                        content = content.replace(m.group(0), "<%" + ResourceFile.open(path).content() + "%>");
                    } else {
                        content = content.replace(m.group(0), ResourceFile.open(path).content());
                    }
                }

                //static site  @ or %
                p = Pattern.compile("\\s(src|href)=\"([@|%])", Pattern.CASE_INSENSITIVE);
                m = p.matcher(content);
                while (m.find()) {
                    content = content.replace(m.group(0), " " + m.group(1) + "=\"" + (m.group(2).equals("@") ? Setting.VoyagerStaticSite : Setting.VoyagerGallerySite));
                }

                HttpRequest http = new HttpRequest(request);
                Object result =
                        new PQL("EMBEDDED:" + content, new DataHub(Properties.contains(Setting.VoyagerConnection) ? Setting.VoyagerConnection : ""))
                                .place(http.getParameters())
                                .place(String.join("&", baseArgs))
                                .set("request", http.getRequestInfo())
                                .set(model)
                                .run();

                if (result != null) {
                    content = result.toString();
                    //title
                    if (content.contains("<title>#{title}</title>") && content.contains("<h1>")) {
                        content = content.replace("<title>#{title}</title>", "<title>" + content.substring(content.indexOf("<h1>") + 4, content.indexOf("</h1>")) + "</title>");
                    }
                }
                else {
                    content = "";
                }

                response.setCharacterEncoding(Setting.VoyagerCharset);
                response
                        .getWriter()
                        .write(content);
            }
            else {
                response
                        .getWriter()
                        .write("<h1>404</h1><p>Can't find '" + url + "' or markdown file with the save name.</p>");
            }
        }
    }
}