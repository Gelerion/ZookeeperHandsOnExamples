package com.gelerion.zookeeper.sessions.master.worker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
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
    private String status;

    public Worker(String arg) {
        hostPort = arg;
    }

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

    StringCallback createWorkerCallback = (rc, path, ctx, name) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                //As we have seen earlier, because we are registering an ephemeral node, if the worker dies the
                //registered znode representing that node will go away. So this is all we need to do on the
                //worker’s side for group membership.
                register();
                break;
            case OK:
                LOG.info("Registered successfully {}", serverId);
                break;
            case NODEEXISTS:
                LOG.info("Already registered: {}", serverId);
                break;
                default:
                    LOG.error("Something went wrong {}", Code.get(rc));
        }
    };

    //------------------------------------------------------------------------------------------------------------------

    // Update status
    //------------------------------------------------------------------------------------------------------------------

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    synchronized private void updateStatus(String status) {
        if (status.equals(this.status)) {
            //We do an unconditional update (the third parameter; the expected version is –1, so version checking is
            //disabled), and we pass the status we are setting as the context object
            zk.setData("/workers/worker-" + serverId, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }

    StatCallback statusUpdateCallback = (rc, path, ctx, name) -> {
        switch (Code.get(rc)) {
            //There is a subtle problem with asynchronous requests that we retry on connection loss: things may get
            //out of order. ZooKeeper is very good about maintaining order for both requests and responses, but a
            //connection loss makes a gap in that ordering, because we are creating a new request. So, before we
            //requeue a status update, we need to make sure that we are requeuing the current status; otherwise,
            //we just drop it. We do this check and retry in a synchronized block.
            case CONNECTIONLOSS:
                updateStatus((String) ctx);
                break;
        }
    };

//    1. The worker starts working on task-1, so it sets the status to working on task-1.
//    2. The client library tries to issue the setData, but encounters networking problems.
//    3. After the client library determines that the connection has been lost with ZooKeeper and before statusUpdateCallback
//       is called, the worker finishes task-1 and becomes idle.
//    4. The worker asks the client library to issue a setData with Idle as the data.
//    5. Then the client processes the connection lost event; if updateStatus does not check the current status, it
//       would then issue a setData with working on task-1.
//    6. When the connection to ZooKeeper is reestablished, the client library faithfully issues the two setData operations
//       in order, which means that the final state would be working on task-1.
    //------------------------------------------------------------------------------------------------------------------


    @Override
    public void process(WatchedEvent event) {
        LOG.info(event.toString() + ", " + hostPort);
    }

    private void stopZK() throws InterruptedException {
        zk.close();
    }

    //127.0.0.1:2181
    public static void main(String[] args) throws IOException, InterruptedException {
        Worker w = new Worker(args[0]);
        w.startZK();

        w.register();

        Thread.sleep(60000);

        w.stopZK();
    }
}
