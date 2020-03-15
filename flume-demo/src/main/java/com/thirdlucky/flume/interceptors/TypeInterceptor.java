package com.thirdlucky.flume.interceptors;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * @author Json Shen
 */
public class TypeInterceptor implements Interceptor {
    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {

        Map<String, String> headers = event.getHeaders();

        String body = new String(event.getBody());
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
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
