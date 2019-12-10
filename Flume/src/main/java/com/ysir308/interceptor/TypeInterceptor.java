package com.ysir308.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 根据传入的事件中是否包含hello来进行拦截
 */

public class TypeInterceptor implements Interceptor {

    private List<Event> addHeaderEvents;

    public void initialize() {
        addHeaderEvents = new ArrayList<Event>();
    }

    /**
     * 单个事件拦截
     *
     * @param event
     * @return
     */
    public Event intercept(Event event) {

        // 获取事件中的头信息
        Map<String, String> headers = event.getHeaders();

        // 获取事件中的body信息
        String body = new String(event.getBody());

        // 根据body中是否有hello来决定添加怎样的头信息
        if (body.contains("hello")) {

            headers.put("type", "isHello");

        } else {
            headers.put("type", "notHello");
        }

        return event;
    }

    /**
     * 批量事件拦截
     *
     * @param list
     * @return
     */
    public List<Event> intercept(List<Event> list) {

        // 清空集合
        addHeaderEvents.clear();

        // 遍历events，给每一个事件添加头信息
        for (Event event : list) {

            addHeaderEvents.add(intercept(event));

        }

        return addHeaderEvents;
    }

    public void close() {

    }


    public static class  Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new TypeInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
