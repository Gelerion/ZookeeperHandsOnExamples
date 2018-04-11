package com.gelerion.zookeeper.sessions.master.worker.v1;

import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by denis.shuvalov on 09/04/2018.
 */
public class AsyncMaster implements Watcher {

    private String hostPort;
    private ZooKeeper zk;
    private CountDownLatch latch = new CountDownLatch(1);

    private AsyncMaster(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    //---------------------------------------------------------------------------------------------------------------
    private String serverId = Integer.toString(ThreadLocalRandom.current().nextInt());
    private boolean isLeader = false;

    void runForMaster() {
        //async create with two additional parameters
        // callback and a user-specified context (an object that will be passed through to the callback when it is invoked)
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                masterCreateCallback, null);
        //Notice that this create doesn’t throw any exceptions, which can simplify things for us. Because the call is
        //not waiting for the create to complete before returning, we don’t have to worry about the InterruptedException
        //because any request errors are encoded in the first parameter in the callback, we don’t have to worry about KeeperException
    }

    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        /**
         * @param rc Returns the result of the call, which will be OK or a code corresponding to a KeeperException
         * @param path The path that we passed to the create
         * @param ctx Whatever context we passed to the create
         * @param name The name of the znode that was created
         *             For now, path and name will be equal if we succeed, but if CreateMode.SEQUENTIAL is used, this will not be true.
         */
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    latch.countDown();
                    isLeader = true;
                    break;
                default:
                    latch.countDown();
                    isLeader = false;
            }
            System.out.println("I'm " + (isLeader ? "" : "not ") + "the leader");
        }
    };

    void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    }

    AsyncCallback.DataCallback masterCheckCallback = (rc, path, ctx, data, stat) -> {
        switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMaster();
                return;
            case NONODE:
                runForMaster();
        }
    };

    //---------------------------------------------------------------------------------------------------------------

    //Setting Up Metadata
    //---------------------------------------------------------------------------------------------------------------
    //We will use the asynchronous API to set up the metadata directories. Our master–worker design depends on three
    //other directories: /tasks, /assign, and /workers. We can count on some kind of system setup to make sure
    //everything is created before the system is started, or a master can make sure these directories are created
    //every time it starts up. The following code segment will create these paths. There isn’t really any error
    //handling in this example apart from handling a connection loss
    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    private void createParent(String path, byte[] data) {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
    }

    private AsyncCallback.StringCallback createParentCallback = (rc, path, ctx, name) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                createParent(path, (byte[]) ctx);
                break;
            case OK:
                System.out.println("Parent created");
                break;
            case NODEEXISTS:
                System.out.println("Parent already registered: " + path);
                break;
            default:
                System.out.println("Something went wrong: " + KeeperException.create(Code.get(rc), path));
        }
    };
    //---------------------------------------------------------------------------------------------------------------

    private void await() throws InterruptedException {
        latch.await();
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }

    //127.0.0.1:2181
    public static void main(String[] args) throws InterruptedException, IOException {
        AsyncMaster m = new AsyncMaster(args[0]);
        m.startZK();

        m.bootstrap();

        m.runForMaster();

        m.await();
        if(m.isLeader) {
            System.out.println("I'm the leader");
            // wait for a bit
            Thread.sleep(600_000);
        }
        else {
            System.out.println("Someone else is the leader");
        }

        m.stopZK();
    }
}
