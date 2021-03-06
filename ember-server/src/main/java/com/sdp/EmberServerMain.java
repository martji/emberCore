package com.sdp;

import com.sdp.common.RegisterHandler;
import com.sdp.config.ConfigManager;
import com.sdp.log.Log;
import com.sdp.server.EmberServer;
import com.sdp.server.EmberServerHandler;

import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author martji
 *         The entrance of ember server, the init work contains two parts: read the config and start the server.
 *         <p>
 *         All the configs are managed by the golbal {@link ConfigManager}, which saved the servers
 *         information{@link com.sdp.server.EmberServerNode} and the configs for
 *         messageManager{@link com.sdp.manager.MessageManager}.
 *         <p>
 *         Before init the ember server{@link EmberServer}, the serverHanders{@link EmberServerHandler} should be
 *         init first, and the serverHanders should be passed to ember server.
 */
public class EmberServerMain {

    /**
     * @param args
     */
    public static void main(String[] args) {
        EmberServerMain emberLauncher = new EmberServerMain();
        emberLauncher.start();
    }

    /**
     * Read the config and server information, and then start the server{@link EmberServer}.
     */
    public void start() {
        ConfigManager.init();
        int id = getServerNumber();
        ConfigManager.setId(id);
        Log.setInstanceId(id);
        Log.init();
        Log.log.info("[Ember server] new ember server instance start, serverId = " + id);

        RegisterHandler.initHandler();
        EmberServer mServer = new EmberServer();
        ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> replicaTable = new ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>>();
        EmberServerHandler wServerHandler = new EmberServerHandler(false, replicaTable, mServer);
        EmberServerHandler rServerHandler = new EmberServerHandler(true, replicaTable, mServer);
        mServer.init(wServerHandler, rServerHandler);
    }

    public int getServerNumber() {
        System.out.print("Please input the serverId:");
        Scanner scanner = new Scanner(System.in);
        return Integer.decode(scanner.next());
    }
}
