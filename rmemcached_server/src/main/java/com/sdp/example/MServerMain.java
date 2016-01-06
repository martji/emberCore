package com.sdp.example;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Vector;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.sdp.common.RegisterHandler;
import com.sdp.replicas.LocalSpots;
import com.sdp.server.MServer;
import com.sdp.server.MServerHandler;
import com.sdp.server.ServerNode;

/**
 * 
 * @author martji
 *
 */
public class MServerMain {

	final int TWOPHASECOMMIT = 1;
	final int PAXOS = 2;
	final int WEAK = 0;
	
	Map<Integer, ServerNode> serversMap;
	int protocol;
	String monitorAddress;
	int threshold;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Log.init();
		MServerMain lanuch = new MServerMain();
		lanuch.start();
	}

	public void start() {
		serversMap = new HashMap<Integer, ServerNode>();
		RegisterHandler.initHandler();
		getConfig();
		getServerList();
		initLocalHotspot();
		int id = getMemcachedNumber();
		Log.setInstanceId(id);
		Log.log.info(Log.id + " new r-memcached instance start, instance id: " + id);
		
		MServer mServer = new MServer();
		ServerNode serverNode = serversMap.get(id);
		String server = serverNode.getServer();
		ConcurrentHashMap<String, Vector<Integer>> replicasIdMap = new ConcurrentHashMap<String, Vector<Integer>>();
		MServerHandler wServerHandler = new MServerHandler(server, id, serversMap, protocol, replicasIdMap);
		MServerHandler rServerHandler = new MServerHandler(server, id, serversMap, protocol, replicasIdMap);
		rServerHandler.setMServer(mServer);
		rServerHandler.replicasMgr.initHotspotIdentifier();
		mServer.init(id, monitorAddress, serversMap, wServerHandler, rServerHandler);
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
	
	public void initLocalHotspot() {
		LocalSpots.threshold = threshold;
	}
	
	public void getConfig() {
		String configPath = System.getProperty("user.dir") + "/config/config.properties";
		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(configPath));
			monitorAddress = properties.getProperty("monitorAddress").toString();
			threshold = Integer.decode(properties.getProperty("threshold"));
			String protocolName = properties.getProperty("consistencyProtocol").toString();
			if(protocolName.equals("twoPhaseCommit")){
				protocol = TWOPHASECOMMIT;
			} else if(protocolName.equals("paxos")){
				protocol = PAXOS;
			} else if(protocolName.equals("weak")){
				protocol = WEAK;
			} else{
				Log.log.error("consistency protocol input error");
			}
		} catch (Exception e) {
			Log.log.error("wrong config.properties", e);
		}
	}
	
	@SuppressWarnings("resource")
	public int getMemcachedNumber() {
		System.out.print("Please input the server number:");
		Scanner scanner = new Scanner(System.in);
		return Integer.decode(scanner.next());
	}
}
