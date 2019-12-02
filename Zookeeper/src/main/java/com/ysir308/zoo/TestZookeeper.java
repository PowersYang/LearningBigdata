package com.ysir308.zoo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestZookeeper {

    private String conn = "localhost:2181,hadoop101:2181:hadoop102:2181";
    private int sessionTimeout = 2000; // 超时时间为2s
    ZooKeeper zkClient;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(conn, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    @Test
    public void createNode() throws KeeperException, InterruptedException {

        String path = zkClient.create("/root", "root data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println(path);
    }

    // 获取子节点并监控数据变化
    @Test
    public void getDataAndWatch() {

        List<String> children = null;
        try {
            children = zkClient.getChildren("/root", true);

            for (String child : children) {
                System.out.println(child);
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void isExist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/root", true);

        System.out.println(stat == null ? "not exist" : "exist");
    }
}
