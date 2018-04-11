package com.gelerion.zookeeper.sessions.state.changes.v2;

import com.gelerion.zookeeper.sessions.common.ChildrenCache;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.zookeeper.CreateMode.EPHEMERAL;

/**
 * Created by denis.shuvalov on 11/04/2018.
 * <p>
 * 1. Mastership changes
 * 2. Master waits for changes to the list of workers
 * 3. Master waits for new tasks to assign
 * 4. Worker waits for new task assignments
 * 5. Client waits for task execution result
 */
public class MasterV2 implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /*
     * A master process can be either running for primary master, elected primary master,
     * or not elected, in which case it is a backup master.
     */
    enum MasterStates {RUNNING, ELECTED, NOT_ELECTED}

    private String hostPort;
    private ZooKeeper zk;
    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    private MasterV2(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    //Bootstrap
    //---------------------------------------------------------------------------------------------------------------
    void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    private void createParent(String path, byte[] data) {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
    }

    private StringCallback createParentCallback = (rc, path, ctx, name) -> {
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

    //Master election
    //---------------------------------------------------------------------------------------------------------------
    private String serverId = Integer.toString(rnd.nextInt());
    private MasterStates state;

    void runForMaster() {
        zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, EPHEMERAL, masterCreateCallback, null);
    }

    StringCallback masterCreateCallback = (rc, path, ctx, name) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMaster();
                break;
            case OK:
                //If OK, then it simply takes leadership.
                state = MasterStates.ELECTED;
                takeLeadership();
                break;
            case NODEEXISTS:
                //If someone else has already created the znode, then the client needs to watch it.
                state = MasterStates.NOT_ELECTED;
                masterExists();
            default:
                //If anything unexpected happens, then it logs the error and doesn’t do anything else.
                state = MasterStates.NOT_ELECTED;
                LOG.error("Something went wrong when running for master.", KeeperException.create(Code.get(rc), path));
        }
    };

    void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    }

    DataCallback masterCheckCallback = (rc, path, ctx, data, stat) -> {
        switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMaster();
                return;
            case NONODE:
                runForMaster();
        }
    };

    //---------------------------------------------------------------------------------------------------------------
    //Mastership Changes
    //---------------------------------------------------------------------------------------------------------------

    private void masterExists() {
        zk.exists("/master", masterExistsWatcher, masterExistsCallback, null);
    }

    Watcher masterExistsWatcher = event -> {
        if(event.getType() == Event.EventType.NodeDeleted) {
            assert "/master".equals(event.getPath());
            //If the /master znode is deleted, then it runs for master again.
            runForMaster();
        }
    };

    /*
    Following the asynchronous style we used in “Getting Mastership Asynchronously”, we also create a callback method
    for the exists call that takes care of a few cases. First, in the case of connection loss, it retries the exists
    operation. Second, it is possible for the /master znode to be deleted between the execution of the create callback
    and the execution of the exists operation. If that happens, then the callback is invoked with NONODE and we run for
    master again. For all other cases, we check for the /master znode by getting its data. The final case is the client
    session expiring. In this case, the callback to get the data of /master logs an error message and exits.
     */
    StatCallback masterExistsCallback = (rc, path, ctx, name) -> {
        switch (Code.get(rc)) {
            case OK:
                break;
            case CONNECTIONLOSS:
                masterExists();
                break;
            case NONODE:
                state = MasterStates.RUNNING;
                runForMaster();
                break;
            default:
                checkMaster();
                break;
        }
    };

    //---------------------------------------------------------------------------------------------------------------

    private void takeLeadership() {
        //TODO:
    }

    //Master Waits for Changes to the List of Workers
    //--------
    /*
    New workers may be added to the system and old workers may be decommissioned at any time. Workers might also crash
    before executing their assignments. To determine the workers that are available at any one time, we register new
    workers with ZooKeeper by adding a znode as a child of /workers. When a worker crashes or is simply removed from
    the system, its session expires, automatically causing its znode to be removed. Workers ideally close their sessions
    without making ZooKeeper wait for a session expiration.
     */

    void getWorkers() {
        zk.getChildren("/workers", workersChangeWatcher, workersGetChildrenCallback, null);
    }

    private Watcher workersChangeWatcher = event -> {
        if(event.getType() == Event.EventType.NodeChildrenChanged) {
            assert "/workers".equals(event.getPath());
            getWorkers();
        }
    };

    private ChildrenCallback workersGetChildrenCallback = (rc, path, ctx, children) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                getWorkers();
                break;
            case OK:
                LOG.info("Successfully got a list of workers: {} workers", children.size());
                reassignAndSet(children);
                break;
            default:
                LOG.error("getChildren failed", KeeperException.create(Code.get(rc), path));
        }
    };

    //Here’s the cache that holds the last set of workers we have seen.
    //We use the cache because we need to remember what we have seen before. Say that we get the list of workers for
    //the first time. When we get the notification that the list of workers has changed, we won’t know what exactly has
    //changed even after reading it again unless we keep the old values. The cache class for this example simply keeps
    //the last list the master has read and implements a couple of methods to determine what has changed.
    private ChildrenCache workersCache;

    //This call reassigns tasks of dead workers and sets the new list of workers.
    private void reassignAndSet(List<String> children) {
        List<String> toProcess;

        if(workersCache == null) {
            workersCache = new ChildrenCache(children);
            //The first time we get workers, there is nothing to do.
            toProcess = null;
        }
        else {
            LOG.info("Removing and setting");
            //If it is not the first time, then we need to check if some worker has been removed.
            toProcess = workersCache.removedAndSet(children);
        }

        if(toProcess != null) {
            for(String worker : toProcess) {
                //If there is any worker that has been removed, then we need to reassign its tasks.
                getAbsentWorkerTasks(worker);
            }
        }
    }

    private void getAbsentWorkerTasks(String worker) {
        //TODO:
    }

    //---------------------------------------------------------------------------------------------------------------

    //Master Waits for New Tasks to Assign
    //---------------------------------------------------------------------------------------------------------------
    //Like waiting for changes to the list of workers, the primary master waits for new tasks to be added to /tasks.
    //The master initially obtains the set of current tasks and sets a watch for changes to the set. The set is
    //represented in ZooKeeper by the children of /tasks, and each child corresponds to a task. Once the master
    //obtains tasks that have not yet been assigned, it selects a worker at random and assigns the task to the worker.
    void getTasks() {
        zk.getChildren("/tasks", tasksChangeWatcher, tasksGetChildrenCallback, null);
    }

    private Watcher tasksChangeWatcher = event -> {
        if(event.getType() == Event.EventType.NodeChildrenChanged) {
            assert "/tasks".equals(event.getPath());
            getTasks();
        }
    };

    ChildrenCallback tasksGetChildrenCallback = (rc, path, ctx, children) -> {
        switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                if(children != null) assignTasks(children);
                break;
            default:
                LOG.error("getChildren failed.", KeeperException.create(Code.get(rc), path));
        }
    };

    private void assignTasks(List<String> children) {
        //TODO:
    }

    void getTaskData(String task) {
        zk.getData("/tasks/" + task, false, taskDataCallback, task);
    }

    DataCallback taskDataCallback = (rc, path, ctx, data, stat) -> {
        switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getTaskData((String) ctx);
                break;
            case OK:
                //choose worker at random
                var worker = rnd.nextInt(workersCache.getList().size());
                var designatedWorker = workersCache.getList().get(worker);
                //Assign task to randomly chosen worker.
                var assignmentPath = String.format("/assign/%s/%s", designatedWorker, ctx);
                createAssignment(assignmentPath, data);
                break;
            default:
                LOG.error("Error when trying to get task data.", KeeperException.create(Code.get(rc), path));
        }
    };

    /*
    For new tasks, after the master selects a worker to assign the task to, it creates a znode under /assign/worker-id,
    where id is the identifier of the worker. Next, it deletes the znode from the list of pending tasks. The code for
    deleting the znode in the previous example follows the pattern of earlier code we have shown.
    When the master creates an assignment znode for a worker with identifier id, ZooKeeper generates a notification for
    the worker, assuming that the worker has a watch registered upon its assignment znode (/assign/worker-id).
    Note that the master also deletes the task znode under /tasks after assigning it successfully. This approach simplifies
    the role of the master when it receives new tasks to assign. If the list of tasks mixed the assigned and unassigned
    tasks, the master would need a way to disambiguate the tasks.
     */

    //We need to get the task data first because we delete the task znode under /tasks after assigning it.
    //This way the master doesn’t have to remember which tasks it has assigned.
    private void createAssignment(String path, byte[] data) {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, assignTaskCallback, data);
    }

    StringCallback assignTaskCallback = (rc, path, ctx, name) -> {
        switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                createAssignment(path, (byte[]) ctx);
                break;
            case OK:
                LOG.info("Task assigned correctly: " + name);
                deleteTask(name.substring( name.lastIndexOf("/") + 1 ));
                break;
            case NODEEXISTS:
                LOG.warn("Task already assigned");
                break;
            default:
                LOG.error("Error when trying to assign task.", KeeperException.create(Code.get(rc), path));
        }
    };

    private void deleteTask(String task) {
        //TODO:
    }

    //---------------------------------------------------------------------------------------------------------------

    //---------------------------------------------------------------------------------------------------------------
    //---------------------------------------------------------------------------------------------------------------


    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }

    //127.0.0.1:2181
    public static void main(String[] args) throws InterruptedException, IOException {
        var m = new MasterV2(args[0]);
        m.startZK();

        m.bootstrap();

        m.runForMaster();

//        if(m.isLeader) {
//            System.out.println("I'm the leader");
//            // wait for a bit
//            Thread.sleep(600_000);
//        }
//        else {
//            System.out.println("Someone else is the leader");
//        }

        m.stopZK();
    }
}
