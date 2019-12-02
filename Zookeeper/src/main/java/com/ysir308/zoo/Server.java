package com.ysir308.zoo;

import org.apache.zookeeper.*;

import java.io.IOException;

public class Server {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        Server server = new Server();

        // 连接zookeeper集群
        server.getConnect();

        // 注册节点，args[0]为主机名称
        server.regist(args[0]);

        // 业务逻辑处理
        server.business();
    }


    private ZooKeeper zkClient;
    private String conn = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private int sessionTimeout = 2000;

    private void getConnect() throws IOException {

        zkClient = new ZooKeeper(conn, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    private void regist(String hostname) throws KeeperException, InterruptedException {

        String path = zkClient.create("/servers/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println(hostname + "is online");
    }

    private void business() {
        System.out.println("业务逻辑");
    }
}
