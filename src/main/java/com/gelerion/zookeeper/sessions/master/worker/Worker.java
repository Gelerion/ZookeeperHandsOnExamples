package com.gelerion.zookeeper.sessions.master.worker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by denis.shuvalov on 09/04/2018.
 */
public class Worker implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    ZooKeeper zk;
    String hostPort;
    String serverId = Integer.toHexString(ThreadLocalRandom.current().nextInt());

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    //------------------------------------------------------------------------------------------------------------------

    void register() {
        zk.create("/workers/worker-" + serverId,
                "Idle".getBytes(),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback,
                null);
    }

    StringCallback createWorkerCallback;

    //------------------------------------------------------------------------------------------------------------------

    @Override
    public void process(WatchedEvent event) {
        LOG.info(event.toString() + ", " + hostPort);
    }
}
