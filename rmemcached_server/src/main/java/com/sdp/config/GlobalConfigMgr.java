package com.sdp.config;

import com.sdp.example.Log;
import com.sdp.server.ServerNode;
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
 */
public class GlobalConfigMgr {

    public static HashMap<String, Object> propertiesMap = new HashMap<String, Object>();
    public static HashMap<Integer, ServerNode> serversMap = new HashMap<Integer, ServerNode>();
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

            int hot_spot_detector_mode = Integer.decode(properties.getProperty(HOT_SPOT_DETECTOR_MODE, "0"));
            int replica_mode = Integer.decode(properties.getProperty(REPLICA_MODE, "0"));

            int slice_time = Integer.decode(properties.getProperty(SLICE_TIME, "15")) * 1000;

            int multi_bloom_filter_number = Integer.decode(properties.getProperty(MULTI_BLOOM_FILTER_NUMBER, "4"));
            int bloom_filter_length = Integer.decode(properties.getProperty(BLOOM_FILTER_LENGTH, "100"));
            double frequent_percent = Double.parseDouble(properties.getProperty(FREQUENT_PERCENT, "0.8"));

            double hot_spot_percentage = Double.parseDouble(properties.getProperty(HOT_SPOT_PERCENTAGE, "0.0001"));
            double hot_spot_influence = Double.parseDouble(properties.getProperty(HOT_SPOT_INFLUENCE, "0.1"));

            int frequent_item_number = Integer.decode(properties.getProperty(FREQUENT_ITEM_NUMBER, "10"));
            int counter_number = Integer.decode(properties.getProperty(COUNTER_NUMBER, "10"));
            double error_rate = Double.parseDouble(properties.getProperty(ERROR_RATE, "0.01"));
            int threshold = Integer.decode(properties.getProperty(THRESHOLD, "10"));


            propertiesMap.put(REPLICA_PROTOCOL, replica_protocol);
            propertiesMap.put(MONITOR_ADDRESS, monitor_address);

            propertiesMap.put(HOT_SPOT_DETECTOR_MODE, hot_spot_detector_mode);
            propertiesMap.put(REPLICA_MODE, replica_mode);

            propertiesMap.put(SLICE_TIME, slice_time);
            
            propertiesMap.put(MULTI_BLOOM_FILTER_NUMBER, multi_bloom_filter_number);
            propertiesMap.put(BLOOM_FILTER_LENGTH, bloom_filter_length);
            propertiesMap.put(FREQUENT_PERCENT, frequent_percent);

            propertiesMap.put(HOT_SPOT_PERCENTAGE, hot_spot_percentage);
            propertiesMap.put(HOT_SPOT_INFLUENCE, hot_spot_influence);

            propertiesMap.put(FREQUENT_ITEM_NUMBER, frequent_item_number);
            propertiesMap.put(TOP_ITEM_NUMBER, frequent_item_number);
            propertiesMap.put(COUNTER_NUMBER, counter_number);
            propertiesMap.put(ERROR_RATE, error_rate);
            propertiesMap.put(THRESHOLD, threshold);
        } catch (Exception e) {
            Log.log.error("wrong config.properties", e);
        }
    }

    public static void initXml() {
        String serverListPath = System.getProperty("user.dir") + "/config/serverlist.xml";
        SAXReader sr = new SAXReader();
        try {
            Document doc = sr.read(serverListPath);
            Element root = doc.getRootElement();
            List<Element> childElements = root.elements();
            for (Element server : childElements) {
                int id = Integer.parseInt(server.elementText("id"));
                String host = server.elementText("host");
                int rport = Integer.parseInt(server.elementText("rport"));
                int wport = Integer.parseInt(server.elementText("wport"));
                int memcached = Integer.parseInt(server.elementText("memcached"));
                ServerNode serverNode = new ServerNode(id, host, rport, wport, memcached);
                serversMap.put(id, serverNode);
            }
        } catch (DocumentException e) {
            Log.log.error("wrong serverlist.xml", e);
        }
    }

    public static void setId (int id) {
        GlobalConfigMgr.id = id;
    }

    public static final String REPLICA_PROTOCOL = "replica_protocol";
    public static final String MONITOR_ADDRESS = "monitor_address";

    public static final String HOT_SPOT_DETECTOR_MODE = "hot_spot_detector_mode";
    public static final String REPLICA_MODE = "replica_mode";

    public static final String SLICE_TIME = "slice_time";

    public static final String MULTI_BLOOM_FILTER_NUMBER = "multi_bloom_filter_number";
    public static final String BLOOM_FILTER_LENGTH = "bloom_filter_length";
    public static final String FREQUENT_PERCENT = "frequent_percent";

    public static final String HOT_SPOT_PERCENTAGE = "hot_spot_percentage";
    public static final String HOT_SPOT_INFLUENCE = "hot_spot_influence";

    public static final String FREQUENT_ITEM_NUMBER = "frequent_item_number";
    public static final String TOP_ITEM_NUMBER = "top_item_number";
    public static final String COUNTER_NUMBER = "counter_number";
    public static final String ERROR_RATE = "error_rate";
    public static final String THRESHOLD = "threshold";

    public static final int DATA_STREAM_MODE = 1;
}
