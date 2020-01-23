package io.qross.app;

import io.qross.setting.Global;
import io.qross.setting.Properties;
import org.springframework.web.servlet.view.AbstractTemplateViewResolver;

import java.util.HashMap;
import java.util.Map;

public class VoyagerResolver extends AbstractTemplateViewResolver {

    public VoyagerResolver() {
        setViewClass(requiredViewClass());

        Properties.loadResourcesFile("/voyager.properties");
        setPrefix(Properties.get("voyager.directory", "/templates/"));
        setSuffix("." + Properties.get("voyager.extension", "html"));

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("connection", Properties.get("voyager.connection", "jdbc.default"));
        attributes.put("charset", Properties.get("voyager.charset", Global.CHARSET()));
        setAttributesMap(attributes);
    }

    public VoyagerResolver(String prefix, String suffix) {
        this();
        setPrefix(prefix);
        setSuffix(suffix);
    }

    @Override
    protected Class<?> requiredViewClass() {
        return Voyager.class;
    }
}
