package com.gelerion.zookeeper.sessions;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Created by denis.shuvalov on 05/04/2018.
 * <p>
 * As long as the session is alive, the handle will remain valid, and the ZooKeeper client library will continually
 * try to keep an active connection to a ZooKeeper server to keep the session alive. If the handle is closed, the
 * ZooKeeper client library will tell the ZooKeeper servers to kill the session. If ZooKeeper decides that a client
 * has died, it will invalidate the session. If a client later tries to reconnect to a ZooKeeper server using the
 * handle that corresponds to the invalidated session, the ZooKeeper server informs the client library that the
 * session is no longer valid and the handle returns errors for all operations
 */
public class CreatingZookeeperSession {

    /**
     * connectString
     *  - Contains the hostnames and ports of the ZooKeeper servers. We listed those servers when we used zkCli to
     *    connect to the ZooKeeper service.
     * <p>
     * sessionTimeout
     *  - The time in milliseconds that ZooKeeper waits without hearing from the client before declaring the session dead.
     *    For now, we will just use a value of 15000, for 15 seconds. This means that if ZooKeeper cannot communicate
     *    with a client for more than 15 seconds, ZooKeeper will terminate the client’s session. Note that this timeout
     *    is rather high, but it is useful for the experiments we will be doing later. ZooKeeper sessions typically
     *    have a timeout of 5–10 seconds.
     * <p>
     * watcher
     *  - An object we need to create that will receive session events. Because Watcher is an interface, we will need
     *    to implement a class and then instantiate it to pass an instance to the ZooKeeper constructor. Clients use
     *    the Watcher interface to monitor the health of the session with ZooKeeper. Events will be generated when a
     *    connection is established or lost to a ZooKeeper server. They can also be used to monitor changes to ZooKeeper
     *    data. Finally, if a session with ZooKeeper expires, an event is delivered through the Watcher interface to
     *    notify the client application.
     *
     *  java  -cp $CLASSPATH Master 127.0.0.1:2181
     */
    public static void main(String[] args) throws InterruptedException, IOException {
        Master m = new Master(args[0]);
        m.startZK();

        // wait for a bit
        Thread.sleep(60000);

        m.stopZK();

    }

    public static class Master implements Watcher {
        ZooKeeper zk;
        String hostPort;

        Master(String hostPort) {
            this.hostPort = hostPort;
        }

        void startZK() throws IOException {
            zk = new ZooKeeper(hostPort, 15000, this);
        }

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
    }

}
