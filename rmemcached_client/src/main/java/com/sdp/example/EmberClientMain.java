package com.sdp.example;

import com.sdp.client.DBClient;
import com.sdp.client.DataClientFactory;
import com.sdp.common.RegisterHandler;
import com.sdp.server.ServerNode;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
/**
 * 
 * @author martji
 *
 */

public class EmberClientMain {

	private Logger log;
	private List<ServerNode> serverNodes;
    private int replicasNum;

    private final int RECORD_COUNT = 100;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		EmberClientMain launch = new EmberClientMain();
		launch.test();
	}

	public void test() {
		String logPath = System.getProperty("user.dir") + "/config/log4j.properties";
		PropertyConfigurator.configure(logPath);
		log = Logger.getLogger( EmberClientMain.class.getName());
		log.info("logPath: " + logPath);
		
		RegisterHandler.initHandler();
		getConfig();
		getServerList();
		
		DBClient client = new DBClient(DataClientFactory.MC_MODE, serverNodes);
        client.initConfig(RECORD_COUNT, 0);
		run(client);
		client.shutdown();
	}
	
	public void run(DBClient client) {
		String key = "user";
		String value = "This is a test of an object blah blah es.";
		for (int i = 0; i < 1090; i++) {
			value += "x";
		}
		
		long begin, end, time;
		
		begin = System.currentTimeMillis();
		for (int i = 0; i < RECORD_COUNT; i++) {
			boolean result = client.set(key + i, value);
			if (!result) {
				System.out.println(">>request: set " + key + i + ", error!");
			}
		}
		end = System.currentTimeMillis();
		time = end - begin;
		System.out.println(RECORD_COUNT + " sets: " + time + "ms");
		
		begin = System.currentTimeMillis();
		for (int i = 0; i < RECORD_COUNT; i++) {
			String result = client.get(key + i);
			if (result == null || result.length() == 0) {
				System.out.println(">>request: get " + key + i + ", error!");
			}
		}
		end = System.currentTimeMillis();
		time = end - begin;
		System.out.println(RECORD_COUNT + " gets: " + time + "ms");
	}

	public void getServerList() {
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
			log.error("wrong servers.xml", e);
		}
	}
	
	public void getConfig() {
		String configPath = System.getProperty("user.dir") + "/config/config.properties";
		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(configPath));
			replicasNum = Integer.parseInt(properties.getProperty("replicasNum"));
		} catch (Exception e) {
			log.error("wrong config.properties", e);
		}
	}
}
