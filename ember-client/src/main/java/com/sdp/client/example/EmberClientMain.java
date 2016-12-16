package com.sdp.client.example;

import com.sdp.client.DBClient;
import com.sdp.client.DataClientFactory;
import com.sdp.common.RegisterHandler;
import com.sdp.log.Log;
import com.sdp.server.ServerNode;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
/**
 * @author martji
 */

public class EmberClientMain {

    private List<ServerNode> serverNodes;

    /**
     * @deprecated
     */
    private int replicasNum;

    private final int CLIENT_MODE = DataClientFactory.MC_TYPE;
    private final int REPLICA_MODE = DataClientFactory.REPLICA_EMBER;
    private final int RECORD_COUNT = 100;

    /**
     * @param args
     */
    public static void main(String[] args) {
        Log.init();
        EmberClientMain launch = new EmberClientMain();
        launch.test();
    }

    private void test() {
        RegisterHandler.initHandler();
        getConfig();
        getServerList();

        DBClient client = new DBClient(CLIENT_MODE, REPLICA_MODE, serverNodes, getProperties());
        client.initConfig(RECORD_COUNT, DBClient.SLICE_HASH_MODE, DBClient.SYNC_SET_MODE);
        runTest(client);
        client.shutdown();
    }

    private void runTest(DBClient client) {
        String key = "user";
        String value = "This is a test of an object blah blah es.";

        long begin, end, time;

        begin = System.currentTimeMillis();
        for (int i = 0; i < RECORD_COUNT; i++) {
            boolean result = client.set(key + i, value);
            if (!result) {
                Log.log.error("request: set " + key + i + ", error!");
            }
        }
        end = System.currentTimeMillis();
        time = end - begin;
        Log.log.info(RECORD_COUNT + " sets: " + time + "ms");

        begin = System.currentTimeMillis();
        for (int i = 0; i < RECORD_COUNT; i++) {
            String result = client.get(key + i);
            if (result == null || !result.equals(value)) {
                Log.log.error("request: get " + key + i + ", error!");
            }
        }
        end = System.currentTimeMillis();
        time = end - begin;
        Log.log.info(RECORD_COUNT + " gets: " + time + "ms");
    }

    private void getServerList() {
        serverNodes = new ArrayList<ServerNode>();
        String serverListPath = System.getProperty("user.dir") + "/config/servers.xml";
        SAXReader sr = new SAXReader();
        try {
            Document doc = sr.read(serverListPath);
            Element root = doc.getRootElement();
            List<Element> childElements = root.elements();
            for (Element server : childElements) {
                int id = Integer.parseInt(server.elementText("id"));
                String host = server.elementText("host");
                int readPort = Integer.parseInt(server.elementText("readPort"));
                int writePort = Integer.parseInt(server.elementText("writePort"));
                int dataPort = Integer.parseInt(server.elementText("dataPort"));
                ServerNode serverNode = new ServerNode(id, host, readPort, writePort, dataPort);
                serverNodes.add(serverNode);
            }
        } catch (DocumentException e) {
            Log.log.error("wrong servers.xml", e);
        }
    }

    private void getConfig() {
        String configPath = System.getProperty("user.dir") + "/config/config.properties";
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(configPath));
            replicasNum = Integer.parseInt(properties.getProperty("replicasNum"));
        } catch (Exception e) {
            Log.log.error("wrong config.properties", e);
        }
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.put("data_shards", 1);
        properties.put("parity_shards", 3);
        return properties;
    }
}
