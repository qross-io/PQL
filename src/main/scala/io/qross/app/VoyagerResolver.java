package io.qross.app;

import org.springframework.web.servlet.view.AbstractTemplateViewResolver;

public class VoyagerResolver extends AbstractTemplateViewResolver {

    public VoyagerResolver() {
        setViewClass(requiredViewClass());
    }

//    public VoyagerResolver(String prefix, String suffix) {
//        this();
//        setPrefix(prefix);
//        setSuffix(suffix);
//    }

    @Override
    protected Class<?> requiredViewClass() {
        return Voyager.class;
    }
}
