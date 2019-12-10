package com.ysir308.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    // 定义全局前缀和后缀
    private String prefix;
    private String subfix;

    public void configure(Context context) {

        // 读取配置文件
        prefix = context.getString("prefix");
        subfix = context.getString("subfix", "ysir308");
    }

    /**
     * 1、接收数据
     * 2、封装为事件
     * 3、将事件传给channel
     *
     * @return
     * @throws EventDeliveryException
     */
    public Status process() throws EventDeliveryException {

        Status status = null;

        try {
            // 接收数据(此处伪造数据)
            for (int i = 0; i < 5; i++) {

                SimpleEvent event = new SimpleEvent();

                // 给事件设置值
                event.setBody((prefix + "--" + i + "--" + subfix).getBytes());

                // 将事件传给channel
                getChannelProcessor().processEvent(event);

                status = Status.READY;
            }
        } catch (Exception e) {

            status = Status.BACKOFF;

            e.printStackTrace();
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return status;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

}
