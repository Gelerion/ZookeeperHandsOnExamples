package com.gelerion.zookeeper.sessions.master.worker.v1;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by denis.shuvalov on 09/04/2018.
 */
public class Master implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    ZooKeeper zk;
    String hostPort;

    private Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }


    //-----------------------------------------------------------------------------------------------------------------
    private String serverId = Integer.toString(ThreadLocalRandom.current().nextInt());
    private boolean isLeader = false;

    /**
     * create throws two exceptions: KeeperException and InterruptedException. We need to make sure we handle these
     * exceptions, specifically the ConnectionLossException (which is a subclass of KeeperException) and InterruptedException.
     * For the rest of the exceptions, we can abort the operation and move on, but in the case of these two exceptions,
     * the create might have actually succeeded, so if we are the master we need to handle them.
     */
    void runForMaster() throws KeeperException, InterruptedException {
        while (true) {
            try {
                zk.create(
                        //The znode we are trying to create is /master. If a znode already exists with that name, the create
                        //will fail. We are going to store the unique ID that corresponds to this server as the data of the /master znode.
                        "/master",
                        //Only byte arrays may be stored as data, so we convert the int to a byte array
                        serverId.getBytes(),
                        //Gives all permissions to everyone. As the name indicates, this is a very unsafe ACL to use in untrusted environments
                        //ZooKeeper provides per-znode ACLs with a pluggable authentication method, so if we need to we can restrict who can do what to which znode
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        //We’ll define an EPHEMERAL znode that will automatically be deleted by ZooKeeper when the session that created it closes or is made invalid
                        CreateMode.EPHEMERAL
                );

                isLeader = true;
                break;
            }
            catch (KeeperException.NodeExistsException e) {
                isLeader = false;
                break;
            }
            catch (KeeperException.ConnectionLossException e) {
                //The ConnectionLossException occurs when a client becomes disconnected from a ZooKeeper server
                //When this exception occurs, it is unknown to the client whether the request was lost before the ZooKeeper
                //servers processed it, or if they processed it but the client did not receive the response
                //ZooKeeper client library will reestablish the connection for future requests, but the process must figure
                //out whether a pending request has been processed or whether it should reissue the request.

                //The developer must figure out the state of the system before proceeding. In case there was a leader election,
                //we want to make sure that we haven’t been made the master without knowing it. If the create actually
                //succeeded, no one else will be able to become master until the acting master dies, and if the acting master
                //doesn’t know it has mastership, no process will be acting as the master.
            }

            if(checkMaster()) break;

            //Our handling of InterruptedException depends on our context. If the InterruptedException will bubble up
            //and eventually close our zk handle, we can let it go up the stack and everything will get cleaned up when
            //the handle is closed. If the zk handle is not closed, we need to figure out if we are the master before
            //rethrowing the exception or asynchronously continuing the operation. This latter case is particularly
            //tricky and requires careful design to handle properly.
        }
    }

   // returns true if there is a master
   boolean checkMaster() throws InterruptedException, KeeperException {
       while (true) {
           try {
               Stat stat = new Stat();
               //Check for an active master by trying to get the data for the /master znode
               //When handling the ConnectionLossException, we must find out which process, if any, has created the
               //master znode, and start acting as the leader if that process is ours. We do this by using the getData method:
               byte[] data = zk.getData(
                       //path of the znode from which we will be getting the data
                       "/master",
                       //Indicates whether we want to hear about future changes to the data returned. If set to true,
                       //we will get events on the Watcher object we set when we created the ZooKeeper handle. There
                       //is another version of this method that takes a Watcher object that will receive an event if
                       //changes happen.
                       false,
                       //structure that the getData method can fill in with metadata about the znode
                       stat
               );
               //This line of the example shows why we need the data used in creating the /master znode:
               //if /master exists, we use the data contained in /master to determine who the leader is.
               isLeader = new String(data).equals(serverId);
               return true;
           }
           catch (KeeperException.NoNodeException e) {
               // no master, so try create again
               return false;
           }
           catch (KeeperException.ConnectionLossException e) {}
       }
   }

    //-----------------------------------------------------------------------------------------------------------------

    /**
     * When the Master finishes, it would be nice if its session went away immediately. This is what the
     * ZooKeeper.close() method does. Once close is called, the session represented by the ZooKeeper object
     * gets destroyed.
     */
    void stopZK() throws IOException, InterruptedException {
        zk.close();
    }

    /**
     * What would have happened if we had started the master without starting the ZooKeeper service? Give it a try.
     * Stop the service, then run Master. What do you see? The last line in the previous output, with the WatchedEvent,
     * is not present. The ZooKeeper library isn’t able to connect to the ZooKeeper server, so it doesn’t tell us anything.
     * <p>
     * Now try starting the server, starting the Master, and then stopping the server while the Master is still
     * running. You should see the SyncConnected event followed by the Disconnected event.
     * <p>
     * When developers see the Disconnected event, some think they need to create a new ZooKeeper handle to reconnect
     * to the service. Do not do that! See what happens when you start the server, start the Master, and then stop
     * and start the server while the Master is still running. You should see the SyncConnected event followed by
     * the Disconnected event and then another SyncConnected event. The ZooKeeper client library takes care of
     * reconnecting to the service for you.
     */
    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }

    //127.0.0.1:2181
    public static void main(String args[]) throws Exception {
        Master m = new Master(args[0]);
        m.startZK();

        m.runForMaster();

        if(m.isLeader) {
            System.out.println("I'm the leader");
            // wait for a bit
            Thread.sleep(60000);
        }
        else {
            System.out.println("Someone else is the leader");
        }

        m.stopZK();
    }
}

