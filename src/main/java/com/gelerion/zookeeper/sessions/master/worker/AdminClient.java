package com.gelerion.zookeeper.sessions.master.worker;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Date;

/**
 * Created by denis.shuvalov on 10/04/2018.
 *
 * Finally, we will write a simple AdminClient that will show the state of the system. One of the nice things
 * about ZooKeeper is that you can use the zkCli utility to look at the state of the system, but usually you will
 * want to write your own admin client to more quickly and easily administer the system. In this example, we will
 * use getData and getChildren to get the state of our master–worker system.
 */
public class AdminClient implements Watcher {

    ZooKeeper zk;
    String hostPort;

    AdminClient(String hostPort) { this.hostPort = hostPort; }

    void start() throws Exception {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void listState() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte[] masterData = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: " + new String(masterData) + " since " + startDate);
        } catch (KeeperException.NoNodeException e) {
            System.out.println("No Master");
        }

        System.out.println("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte[] data = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            System.out.println("\t" + w + ": " + state);
        }

        System.out.println("Tasks:");
        for (String t: zk.getChildren("/tasks", false)) {
            System.out.println("\t" + t);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }

    public static void main(String args[]) throws Exception {
        AdminClient c = new AdminClient(args[0]);

        c.start();

        c.listState();
    }
}
