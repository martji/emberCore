package com.sdp.client;

import java.util.HashMap;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.sdp.common.RegisterHandler;
import com.sdp.server.ServerNode;

public class TestConnect2Server {

	/**
	 * @param args
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		int clientId = (int) System.nanoTime();
		int mode = 0;
		int recordCount = 1000;
		HashMap<Integer, ServerNode> serversMap = new HashMap<Integer, ServerNode>();
		RegisterHandler.initHandler();
		String serverListPath = System.getProperty("user.dir") + "/config/servers.xml";
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
				 int memcachedPort = Integer.parseInt(server.elementText("memcached"));
				 ServerNode serverNode = new ServerNode(id, host, rport, wport, memcachedPort);
				 serversMap.put(id, serverNode);
	        }
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		
		RMClient client = new RMClient(clientId, mode, recordCount, serversMap);
		long start = System.currentTimeMillis();
		int count = 10000;
		for (int i = 0; i < count; i++) {
//			mcClient.set2R("test", "123");
			client.set("test", "123");
		}
		System.out.println((System.currentTimeMillis() - start));
		
		client.shutdown();
	}

}
