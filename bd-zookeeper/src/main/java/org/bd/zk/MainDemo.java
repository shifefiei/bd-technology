package org.bd.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Created by shifeifei on 2020/3/24.
 */
public class MainDemo {

    private static String url = "centos-01:2181,centos-02:2181,centos-03:2181";
    private static ZooKeeper zkClient;

    public static void main(String[] args) throws Exception {

        zkClient = new ZooKeeper(url, 2000, new Watcher() {
            public void process(WatchedEvent event) {

                // 收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println(event.getType() + "--" + event.getPath());

                // 再次启动监听
                try {
                    zkClient.getChildren("/", true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }

}
