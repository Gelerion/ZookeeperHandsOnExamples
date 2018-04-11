package com.gelerion.zookeeper.sessions.state.changes.v2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by denis.shuvalov on 09/04/2018.
 */
public class WorkerV2 implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString(rnd.nextInt());
    private String status;

    WorkerV2(String arg) {
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

    //------------------------------------------------------------------------------------------------------------------
    /*
    We similarly create a znode /assign/worker-id so that the master can assign tasks to this worker. If we create
    /workers/worker-id before /assign/worker-id, we could fall into the situation in which the master tries to assign
    the task but cannot because the assigned parent’s znode has not been created yet. To avoid this situation, we need
    to create /assign/worker-id first. Moreover, the worker needs to set a watch on /assign/worker-id to receive a
    notification when a new task is assigned.
    Once the worker has tasks assigned to it, it fetches the tasks from /assign/worker-id and executes them. The worker
    takes each task in its list and verifies whether it has already queued the task for execution. It keeps a list of
    ongoing tasks for this purpose. Note that we loop through the assigned tasks of a worker in a separate thread to
    release the callback thread. Otherwise, we would be blocking other incoming callbacks.
     */

    void getTasks() {
        zk.getChildren("/assign/worker-" + serverId, newTaskWatcher, tasksGetChildrenCallback, null);
    }

    Watcher newTaskWatcher = event -> {
        if(event.getType() == EventType.NodeChildrenChanged) {
            assert ("/assign/worker-" + serverId).equals( event.getPath() );
            getTasks();
        }
    };

    DataCallback taskDataCallback = (rc, path, ctx, data, stat) -> {
        //TODO:

    };

    private Executor executor = Executors.newSingleThreadExecutor();
    private final List<String> onGoingTasks = new ArrayList<>();

    ChildrenCallback tasksGetChildrenCallback = (rc, path, ctx, children) -> {
        switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                if(children != null) {
                    executor.execute(new Runnable() {
                        List<String> children;
                        DataCallback cb;

                        Runnable init(List<String> children, DataCallback cb) {
                            this.children = children;
                            this.cb = cb;
                            return this;
                        }

                        @Override
                        public void run() {
                            LOG.info("Looping into tasks");
                            synchronized (onGoingTasks) {
                                for (var task : children) {
                                    if (!onGoingTasks.contains(task)) {
                                        LOG.trace("New task: {}", task);
                                        zk.getData("/assign/worker-" + serverId + "/" + task, false, cb, task);
                                        onGoingTasks.add(task);
                                    }
                                }
                            }

                        }
                    }.init(children, taskDataCallback));
                }
                break;
            default:
                System.out.println("getChildren failed: " + KeeperException.create(Code.get(rc), path));
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
            case CONNECTIONLOSS:
                updateStatus((String) ctx);
                break;
        }
    };

    @Override
    public void process(WatchedEvent event) {
        LOG.info(event.toString() + ", " + hostPort);
    }

    private void stopZK() throws InterruptedException {
        zk.close();
    }

    //127.0.0.1:2181
    public static void main(String[] args) throws IOException, InterruptedException {
        WorkerV2 w = new WorkerV2(args[0]);
        w.startZK();

        w.register();

        Thread.sleep(600_000);

        w.stopZK();
    }
}
