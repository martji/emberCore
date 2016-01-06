package com.sdp.example;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.sdp.client.MClient;
import com.sdp.common.RegisterHandler;
import com.sdp.server.ServerNode;
/**
 * 
 * @author martji
 *
 */

public class MClientMain {

	Logger log;
	Map<Integer, ServerNode> serversMap;
	int replicasNum;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MClientMain launch = new MClientMain();
		launch.test();
	}

	public void test() {
		String logPath = System.getProperty("user.dir") + "/config/log4j.properties";
		PropertyConfigurator.configure(logPath);
		log = Logger.getLogger( MClientMain.class.getName());
		log.info("logPath: " + logPath);
		serversMap = new HashMap<Integer, ServerNode>();
		
		RegisterHandler.initHandler();
		getConfig();
		getServerList();
		
		int clientId = (int) System.nanoTime();
		MClient mc = new MClient(clientId, serversMap);
		run(mc);
		mc.shutdown();
	}
	
	public void muiltTest(int threadCount) {
		String logPath = System.getProperty("user.dir") + "/config/log4j.properties";
		PropertyConfigurator.configure(logPath);
		log = Logger.getLogger( MClientMain.class.getName());
		log.info("logPath: " + logPath);
		serversMap = new HashMap<Integer, ServerNode>();
		
		RegisterHandler.initHandler();
		getConfig();
		getServerList();
		
		for (int i = 0; i < threadCount; i++) {
			MClient mc = new MClient(i);
			mc.init(serversMap);
			
			new SingleThread(mc, i).start();
		}
	}
	
	class SingleThread extends Thread {
		MClient mc;
		int id;
		public SingleThread(MClient mc, int id) {
			this.mc = mc;
			this.id = id;
		}
		
		public void run() {
			String key = id + "testKey";
			String value = "This is a test of an object blah blah es.";
			
			int runs = 10000;
			int start = 0;
			long begin, end, time;
			
			begin = System.currentTimeMillis();
			for (int i = start; i < start+runs; i++) {
				boolean result = mc.set(key + i, value);
				if (!result) {
					System.out.println(id + ">>request: set " + key + i + ", error!");
				}
			}
			end = System.currentTimeMillis();
			time = end - begin;
			System.out.println("thread" + id + " " + runs + " sets: " + time + "ms");
			
			begin = System.currentTimeMillis();
			for (int i = start; i < start+runs; i++) {
				String result = mc.get(key + i);
				if (result.isEmpty()) {
					System.out.println(id + ">>request: get " + key + i + ", error!");
				}
			}
			end = System.currentTimeMillis();
			time = end - begin;
			System.out.println("thread" + id + " " + runs + " gets: " + time + "ms");
		}
	}
	
	public void run(MClient mc) {
		String key = "testKey";
		String value = "This is a test of an object blah blah es.";
		for (int i = 0; i < 1090; i++) {
			value += "x";
		}
		
		int runs = 1000;
		int start = 0;
		long begin, end, time;
		
		begin = System.currentTimeMillis();
		for (int i = start; i < start+runs; i++) {
			boolean result = mc.set(key + i, value);
			if (!result) {
				System.out.println(">>request: set " + key + i + ", error!");
			}
		}
		end = System.currentTimeMillis();
		time = end - begin;
		System.out.println(runs + " sets: " + time + "ms");
		
		begin = System.currentTimeMillis();
		for (int i = start; i < start+runs; i++) {
			String result = mc.get(key + i);
			if (result == null || result.isEmpty()) {
				System.out.println(">>request: get " + key + i + ", error!");
			}
		}
		end = System.currentTimeMillis();
		time = end - begin;
		System.out.println(runs + " gets: " + time + "ms");
	}
	
	@SuppressWarnings({ "unchecked" })
	public void getServerList() {
		String serverListPath = System.getProperty("user.dir") + "/config/serverlist.xml";
		SAXReader sr = new SAXReader();
		try {
			Document doc = sr.read(serverListPath);
			Element root = doc.getRootElement();
			List<Element> childElements = root.elements();
	        for (Element server : childElements) {
				 int id = Integer.parseInt(server.elementText("id"));
				 String host = server.elementText("host");
				 int port = Integer.parseInt(server.elementText("port"));
				 int memcachedPort = Integer.parseInt(server.elementText("memcached"));
				 ServerNode serverNode = new ServerNode(id, host, port, memcachedPort);
				 serversMap.put(id, serverNode);
	        }
		} catch (DocumentException e) {
			log.error("wrong serverlist.xml", e);
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
