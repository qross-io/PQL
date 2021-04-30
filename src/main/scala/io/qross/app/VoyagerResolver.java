package io.qross.app;

import org.springframework.web.servlet.view.AbstractTemplateViewResolver;

public class VoyagerResolver extends AbstractTemplateViewResolver {

    public VoyagerResolver() {
        setViewClass(requiredViewClass());

        setPrefix(Setting.VoyagerDirectory);
        setSuffix(".html");

//        Map<String, Object> attributes = new HashMap<>();
//        attributes.put("connection", Setting.VoyagerConnection);
//        attributes.put("charset", Setting.VoyagerCharset);
//        setAttributesMap(attributes);
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
