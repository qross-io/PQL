package io.qross.app;

import io.qross.core.DataHub;
import io.qross.ext.TypeExt;
import io.qross.fs.ResourceFile;
import io.qross.net.HttpRequest;
import io.qross.net.Json;
import io.qross.pql.PQL;
import io.qross.pql.Solver;
import io.qross.setting.Language;
import io.qross.setting.Properties;
import org.springframework.web.servlet.view.AbstractTemplateView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class  Voyager extends AbstractTemplateView {

    //Cache file content on first access
    public static Map<String, String> Cache = new HashMap<>();

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
            }
            else {
                dir = url.substring(0, url.indexOf(name));
            }

            String md = url.replaceAll("(?i)\\.html$", ".md");
            String htm = url.replaceAll("(?i)\\.html$", ".htm");

            String content = null;
            boolean markdown = false;

            if (Setting.VoyagerCacheEnabled) {
                if (Voyager.Cache.containsKey(url)) {
                    content = Voyager.Cache.get(url);
                }
                else if (Voyager.Cache.containsKey(md)) {
                    markdown = true;
                    content = Voyager.Cache.get(md);
                }
                else if (Voyager.Cache.containsKey(htm)) {
                    content = Voyager.Cache.get(htm);
                }
            }

            if (content == null) {
                //html frist
                ResourceFile file = ResourceFile.open(url);

                //markdown
                if (!file.exists()) {
                    file = ResourceFile.open(md);
                    if (file.exists()) {
                        markdown = true;
                        content = file.content();
                        if (Setting.VoyagerCacheEnabled) {
                            Voyager.Cache.put(md, content);
                        }
                    }
                }
                else {
                    content = file.content();
                    if (Setting.VoyagerCacheEnabled) {
                        Voyager.Cache.put(url, content);
                    }
                }

                //.htm
                if (!file.exists()) {
                    file = ResourceFile.open(htm);
                }

                if (file.exists()) {
                    content = file.content();
                    if (Setting.VoyagerCacheEnabled) {
                        Voyager.Cache.put(htm, content);
                    }
                }
            }

            if (content != null) {

                String template = "";
                String setup = "";
                List<String> baseArgs = new ArrayList<>();
                String extArgs = "";

                HttpRequest http = new HttpRequest(request);
                Map<String, Object> queries = http.getParameters();

                content = Solver.Sentence$Solver(content).replaceArguments(queries); //replace parameters at first

                //setup
                Pattern p = Pattern.compile("<%!(.*?)%>", Pattern.DOTALL);
                Matcher m = p.matcher(content);
                if (m.find()) {
                    setup = m.group(1);
                    content = content.replace(m.group(0), "");
                }
                
                p = Pattern.compile("<#\\s*page\\s+template=[\"'](.+?)[\"']\\s*/>", Pattern.CASE_INSENSITIVE);
                m = p.matcher(content);
                if (m.find()) {
                    content = content.replace(m.group(0), "");
                    String uri = m.group(1);
                    if (uri.contains("?")) {
                        baseArgs.add(uri.substring(uri.indexOf("?") + 1));
                        uri = uri.substring(0, uri.indexOf("?"));
                    }

                    template = ResourceFile.open((dir + uri).replace("//", "/")).content();
                }                               
                
                // @{ "name": "value" }
                p = Pattern.compile("@(\\{[\\s\\S]+?})(?=\\s*\\n)");
                m = p.matcher(content);
                while (m.find()) {
                    String match = m.group(1);
                    if (match.contains(":")) {
                        int stack = TypeExt.StringExt(match).stackAllPairOf("\\{", "\\}", 0);
                        if (stack > 0) {
                            int index = content.indexOf(match);
                            int pair = TypeExt.StringExt(content).indexPairOf("{", "}", index + match.length(), stack);
                            if (pair > -1) {
                                match += content.substring(index, pair + 1);
                            }
                        }
                        extArgs = match;
                        content = content.replace("@" + match, "");
                    }
                }

                if (markdown) {
                    //markdown to html
                    content = Marker.open(content).transform().colorCodes().getContent();
                    if (template.isEmpty()) {
                        content = Cogo.template().replace("#{content}", content);
                    }
                }

                if (!template.isEmpty()) {
                    content = template.replace("#{content}", content);
                }

                //scripts
                if (content.contains("#{scripts}")) {
                    content = content.replace("#{scripts}", Cogo.getScripts(content));
                }

                //setup
                if (!setup.isEmpty()) {
                    content = "<%" + setup + "%>" + content;
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
                    }
                    else if (path.endsWith(".md")) {
                        content = content.replace(m.group(0), Marker.openFile(path).transform().colorCodes().getContent());
                    }
                    else {
                        content = content.replace(m.group(0), ResourceFile.open(path).content());
                    }
                }

                //language module
                p = Pattern.compile("<#\\s*include\\s+language=[\"'](.+?)[\"']\\s*/>", Pattern.CASE_INSENSITIVE);
                m = p.matcher(content);
                List<String> languageModules = new ArrayList<>();
                while (m.find()) {
                    Arrays.asList(m.group(1).split(",")).forEach(lan -> {
                        lan = lan.trim();
                        if (!lan.isEmpty()) {
                            languageModules.add(lan);
                        }
                    });
                    content = content.replace(m.group(0), "");
                }

                Map<String, Object> args = new HashMap<>(queries);
                if (!baseArgs.isEmpty()) {
                    args.putAll(TypeExt.StringExt(String.join(",", baseArgs)).splitToJavaMap("&", "="));
                }
                if (!extArgs.isEmpty()) {
                    args.putAll(Json.fromText(extArgs).parseJavaMap("/"));
                }
                content = Solver.Sentence$Solver(content).replaceArguments(args);

                //language holder
                p = Pattern.compile("#\\s*([a-z0-9-]+(\\.[a-z0-9-]+)*)\\s*#", Pattern.CASE_INSENSITIVE);
                m = p.matcher(content);
                while (m.find()) {
                    String replacement = Language.get(m.group(1), languageModules);
                    if (replacement != null) {
                        content = content.replace(m.group(0), replacement);
                    }
                }
                languageModules.clear();

                //static site  @ or %
                p = Pattern.compile("\\s(src|href)=\"([@|%])", Pattern.CASE_INSENSITIVE);
                m = p.matcher(content);
                while (m.find()) {
                    content = content.replace(m.group(0), " " + m.group(1) + "=\"" + (m.group(2).equals("@") ? Setting.VoyagerStaticSite : Setting.VoyagerGallerySite));
                }

                try {
                    Object result =
                            new PQL(content, true, new DataHub(Properties.contains(Setting.VoyagerConnection) ? Setting.VoyagerConnection : ""))
                                    .set(args)
                                    .set("request", http.getRequestInfo())
                                    .set(model)
                                    .run();
//                                    .place(queries)
//                                    .place(String.join("&", baseArgs))
//                                    .place(extArgs)

                    if (result != null) {
                        content = result.toString();
                        //title
                        if (content.contains("#{title}") && content.contains("<h1>")) {
                            content = content.replace("#{title}", content.substring(content.indexOf("<h1>") + 4, content.indexOf("</h1>")));
                        }
                    } else {
                        content = "";
                    }
                }
                catch (Exception e) {
                    content = "<h1>500</h1><p>" + TypeExt.ExceptionExt(e).getReferMessage() + "</p>";
                    //content += "<p style=\"color: #CC0000\">" + TypeExt.ExceptionExt(e).getFullMessage() + "</p>";
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