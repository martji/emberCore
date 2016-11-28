package com.sdp.config;

import com.sdp.log.Log;
import com.sdp.server.EmberServerNode;
import com.sun.crypto.provider.DESCipher;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * Created by magq on 16/1/14.
 * The Config contains the {@link EmberServerNode} information in servers.xml and
 * the properties for {@link com.sdp.manager.MessageManager} in config.properties.
 */
public class ConfigManager {

    public static HashMap<String, Object> propertiesMap = new HashMap<String, Object>();
    public static HashMap<Integer, EmberServerNode> serversMap = new HashMap<Integer, EmberServerNode>();
    public static int id = 0;

    public static void init() {
        initProperties();
        initXml();
    }

    public static void initProperties() {
        String configPath = System.getProperty("user.dir") + "/config/config.properties";
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(configPath));

            int replica_protocol = Integer.decode(properties.getProperty(REPLICA_PROTOCOL, "0"));
            String monitor_address = properties.getProperty(MONITOR_ADDRESS, "127.0.0.1").toString();

            int data_client_mode = Integer.decode(properties.getProperty(DATA_CLIENT_MODE, DefaultConfig.DATA_CLIENT_MODE));

            int replica_mode = Integer.decode(properties.getProperty(REPLICA_MODE, DefaultConfig.REPLICA_MODE));
            int update_status_time = Integer.decode(properties.getProperty(UPDATE_STATUS_TIME, DefaultConfig.UPDATE_STATUS_TIME)) * 1000;
            int hot_spot_buffer_size = Integer.decode(properties.getProperty(HOT_SPOT_BUFFER_SIZE, DefaultConfig.HOT_SPOT_BUFFER_SIZE));

            int hot_spot_manager_mode = Integer.decode(properties.getProperty(HOT_SPOT_MANAGER_MODE, DefaultConfig.HOT_SPOT_MANAGER_MODE));
            int slice_time = Integer.decode(properties.getProperty(SLICE_TIME, DefaultConfig.SLICE_TIME)) * 1000;

            int hot_spot_threshold = Integer.decode(properties.getProperty(HOT_SPOT_THRESHOLD, DefaultConfig.HOT_SPOT_THRESHOLD));
            double hot_spot_percentage = Double.parseDouble(properties.getProperty(HOT_SPOT_PERCENTAGE, DefaultConfig.HOT_SPOT_PERCENTAGE));
            double hot_spot_influence = Double.parseDouble(properties.getProperty(HOT_SPOT_INFLUENCE, DefaultConfig.HOT_SPOT_INFLUENCE));

            int bloom_filter_number = Integer.decode(properties.getProperty(BLOOM_FILTER_NUMBER, DefaultConfig.BLOOM_FILTER_NUMBER));
            int bloom_filter_length = Integer.decode(properties.getProperty(BLOOM_FILTER_LENGTH, DefaultConfig.BLOOM_FILTER_LENGTH));
            double frequent_percentage = Double.parseDouble(properties.getProperty(FREQUENT_PERCENTAGE, DefaultConfig.FREQUENT_PERCENTAGE));

            int counter_number = Integer.decode(properties.getProperty(COUNTER_NUMBER, DefaultConfig.COUNTER_NUMBER));
            int top_item_number = Integer.decode(properties.getProperty(TOP_ITEM_NUMBER, DefaultConfig.TOP_ITEM_NUMBER));

            int frequent_threshold = Integer.decode(properties.getProperty(FREQUENT_THRESHOLD, DefaultConfig.FREQUENT_THRESHOLD));

            int interval = Integer.decode(properties.getProperty(INTERVAL, DefaultConfig.INTERVAL));

            double error_rate = Double.parseDouble(properties.getProperty(ERROR_RATE, DefaultConfig.ERROR_RATE));

            propertiesMap.put(REPLICA_PROTOCOL, replica_protocol);
            propertiesMap.put(MONITOR_ADDRESS, monitor_address);

            propertiesMap.put(DATA_CLIENT_MODE, data_client_mode);

            propertiesMap.put(REPLICA_MODE, replica_mode);
            propertiesMap.put(UPDATE_STATUS_TIME, update_status_time);
            propertiesMap.put(HOT_SPOT_BUFFER_SIZE, hot_spot_buffer_size);

            propertiesMap.put(HOT_SPOT_MANAGER_MODE, hot_spot_manager_mode);
            propertiesMap.put(SLICE_TIME, slice_time);

            propertiesMap.put(HOT_SPOT_THRESHOLD, hot_spot_threshold);
            propertiesMap.put(HOT_SPOT_PERCENTAGE, hot_spot_percentage);
            propertiesMap.put(HOT_SPOT_INFLUENCE, hot_spot_influence);

            propertiesMap.put(BLOOM_FILTER_NUMBER, bloom_filter_number);
            propertiesMap.put(BLOOM_FILTER_LENGTH, bloom_filter_length);
            propertiesMap.put(FREQUENT_PERCENTAGE, frequent_percentage);

            propertiesMap.put(COUNTER_NUMBER, counter_number);
            propertiesMap.put(TOP_ITEM_NUMBER, top_item_number);

            propertiesMap.put(FREQUENT_THRESHOLD, frequent_threshold);

            propertiesMap.put(INTERVAL, interval);

            propertiesMap.put(ERROR_RATE, error_rate);
        } catch (Exception e) {
            Log.log.error("wrong config.properties", e);
        }
    }

    public static void initXml() {
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
                EmberServerNode serverNode = new EmberServerNode(id, host, readPort, writePort, dataPort);
                serversMap.put(id, serverNode);
            }
        } catch (DocumentException e) {
            Log.log.error("wrong servers.xml", e);
        }
    }

    public static void setId (int id) {
        ConfigManager.id = id;
    }

    public static final String REPLICA_PROTOCOL = "replica_protocol";
    public static final String MONITOR_ADDRESS = "monitor_address";

    public static final String DATA_CLIENT_MODE = "data_client_mode";

    public static final String REPLICA_MODE = "replica_mode";
    public static final String UPDATE_STATUS_TIME = "update_status_time";
    public static final String HOT_SPOT_BUFFER_SIZE = "hot_spot_buffer_size";

    public static final String HOT_SPOT_MANAGER_MODE = "hot_spot_manager_mode";
    public static final String SLICE_TIME = "slice_time";

    public static final String HOT_SPOT_THRESHOLD = "hot_spot_threshold";
    public static final String HOT_SPOT_PERCENTAGE = "hot_spot_percentage";
    public static final String HOT_SPOT_INFLUENCE = "hot_spot_influence";

    public static final String BLOOM_FILTER_NUMBER = "bloom_filter_number";
    public static final String BLOOM_FILTER_LENGTH = "bloom_filter_length";
    public static final String FREQUENT_PERCENTAGE = "frequent_percentage";

    public static final String COUNTER_NUMBER = "counter_number";
    public static final String TOP_ITEM_NUMBER = "top_item_number";

    public static final String FREQUENT_THRESHOLD = "frequent_threshold";

    public static final String INTERVAL = "interval";

    public static final String ERROR_RATE = "error_rate";
}
