package com.gelerion.zookeeper.sessions.master.worker.v1;

import org.apache.zookeeper.*;

/**
 * Created by denis.shuvalov on 10/04/2018.
 *
 * The final component of the system is the Client application that queues new tats to be executed on a worker
 *
 * We will be adding znodes under /tasks that represents commands to be carries out on the worker
 * We weill be using sequential znodes, which gives us two benefits:
 *  - the sequence number will indicate the order in which the tasks were queued.
 *  - the sequence number will create unique paths for tasks with minimal work
 */
public class Client implements Watcher {
    ZooKeeper zk;
    String hostPort;

    Client(String hostPort) { this.hostPort = hostPort; }

    void startZK() throws Exception {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    String queueCommand(String command) throws KeeperException, InterruptedException {
        while (true) {
            String name = "";
            try {
                //Because we can’t be sure what the sequence number will be when we call create with CreateMode.SEQUENTIAL,
                //the create method returns the name of the new znode.
                name = zk.create(
                        //Because we are using CreateMode.SEQUENTIAL, a monotonically increasing suffix will be
                        //appended to task-. This guarantees that a unique name will be created for each new task
                        //and the task ordering will be established by ZooKeeper.
                        "/tasks/task-",
                        command.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL
                );

                return name;
            } catch (KeeperException.NodeExistsException e) {
                throw new RuntimeException(name + " already appears to be running");
            } catch (KeeperException.ConnectionLossException e) {
                //If we lose a connection while we have a create pending, we will simply retry the create. This may
                //create multiple znodes for the task, causing it to be created twice. For many applications, this
                //execute-at-least-once policy may work fine. Applications that require an execute-at-most-once policy
                //must do more work: we would need to create each of our task znodes with a unique ID (the session ID,
                //for example) encoded in the znode name. We would then retry the create only if a connection loss
                //exception happened and there were no znodes under /tasks with the session ID in their name.
            }

        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("event = " + event);
    }

    public static void main(String[] args) throws Exception {
        //When we run the Client application and pass a command, a new znode will be created in /tasks.
        //It is not an ephemeral znode, so even after the Client application ends, any znodes it has created will remain.
        Client c = new Client(args[0]);

        c.startZK();

        String name = c.queueCommand(args[1]);
        System.out.println("Created " + name);
    }
}
