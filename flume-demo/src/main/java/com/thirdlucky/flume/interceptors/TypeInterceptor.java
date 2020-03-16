package com.thirdlucky.flume.interceptors;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Json Shen
 */
public class TypeInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory
            .getLogger(TypeInterceptor.class);
    @Override
    public void initialize() {
        logger.info("自定义拦截器初始化");
    }

    @Override
    public Event intercept(Event event) {


        Map<String, String> headers = event.getHeaders();

        String body = new String(event.getBody());
        logger.info("自定义拦截器执行"+body);
        if (body.contains("hello")) {
            headers.put("topic", "first");
        } else {
            headers.put("topic","second");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        list.forEach((event)->{intercept(event);});
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            logger.info("创建拦截器");
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {
            String param = context.getString("param");
            logger.info("----------configure方法执行参数：" + param);
        }
    }
}
