package com.ysir308.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {

    private Logger logger = LoggerFactory.getLogger(MySink.class);

    // 定义前缀，后缀
    private String prefix;
    private String subfix;

    public void configure(Context context) {

        // 读取配置文件
        prefix = context.getString("prefix");
        subfix = context.getString("subfix", "ysir308");

    }

    /**
     * 在这里搞事情
     * 1、获取Channel
     * 2、从Channel获取事务和数据
     * 3、发送数据
     *
     * @return
     * @throws EventDeliveryException
     */
    public Status process() throws EventDeliveryException {

        Status status = null;

        Channel channel = getChannel();

        // 获取事务
        Transaction transaction = channel.getTransaction();

        // 开启事务
        transaction.begin();

        try {
            // 从事务获取数据
            Event event = channel.take();

            // 处理事务（业务逻辑）
            if (event != null) {
                String body = new String(event.getBody());
                logger.info(prefix + body + subfix);

                // 提交事务
                transaction.commit();
            }

            // 成功提交，修改状态信息
            status = Status.READY;

        } catch (ChannelException e) {

            // 失败
            transaction.rollback();
            status = Status.BACKOFF;

            e.printStackTrace();
        } finally {

            // 关闭事务
            transaction.close();
        }


        return status;
    }
}
