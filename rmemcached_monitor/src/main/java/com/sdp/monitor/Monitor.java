package com.sdp.monitor;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sdp.common.EMSGID;
import com.sdp.messageBody.CtsMsg.nr_cpuStats;
import com.sdp.netty.NetMsg;

/**
 * 
 * @author martji
 *
 */
public class Monitor extends Thread {
	
	ConcurrentHashMap<Integer, Channel> serverChannelMap = new ConcurrentHashMap<Integer, Channel>();
	Map<Integer, Queue<Double>> cpuCostMap = new HashMap<Integer, Queue<Double>>();
	Map<Integer, Double> medianCpuCostMap = new HashMap<Integer, Double>();
	
	public Monitor() {
		
	}

	public void run() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		while (true) {
			try {
				System.out.println(df.format(new Date()) + ": cpuCost: " + medianCpuCostMap.toString());
				Thread.sleep(1000*5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			Iterator<Entry<Integer, Channel>> servers = serverChannelMap.entrySet().iterator();
			while (servers.hasNext()) {
				Entry<Integer, Channel> server = servers.next();
				Channel channel = server.getValue();
				if (channel.isConnected()) {
					nr_cpuStats.Builder builder = nr_cpuStats.newBuilder();
					NetMsg send = NetMsg.newMessage();
					send.setMessageLite(builder);
					send.setMsgID(EMSGID.nr_stats);
					channel.write(send);
				} else {
					int serverId = server.getKey();
					System.out.println(df.format(new Date()) + ": [Netty] Server " + serverId + " lose connect.");
					serverChannelMap.remove(serverId);
					medianCpuCostMap.remove(serverId);
					cpuCostMap.remove(serverId);
				}
			}
		}
	}

	public void addServer(int clientNode, Channel channel) {
		serverChannelMap.put(clientNode, channel);
	}

	/**
	 * 
	 * @param clientNode
	 * @param cpuCost
	 */
	public void handle(int clientNode, String cpuCost) {
		Double cost = Double.parseDouble(cpuCost);
		if (cpuCostMap.containsKey(clientNode)) {
			Queue<Double> arryCpuCost = cpuCostMap.get(clientNode);
			arryCpuCost.offer(cost);
			if (arryCpuCost.size() > 10) {
				arryCpuCost.poll();
			}
			medianCpuCostMap.put(clientNode, getData((medianCpuCostMap.get(clientNode) + cost) / 2));
		} else {
			Queue<Double> arryCpuCost = new LinkedList<Double>();
			arryCpuCost.offer(cost);
			cpuCostMap.put(clientNode, arryCpuCost);
			medianCpuCostMap.put(clientNode, getData(cost));
		}
	}

	/**
	 * 
	 * @param clientNode
	 */
	public int chooseReplica(int clientNode) {
		int replicaNum = -1;
		Iterator<Entry<Integer, Double>> medianCosts = medianCpuCostMap.entrySet().iterator();
		while (medianCosts.hasNext()) {
			Entry<Integer, Double> costMap = medianCosts.next();
			if (costMap.getKey() != clientNode) {
				if (replicaNum == -1) {
					replicaNum = costMap.getKey();
				} else if (costMap.getValue() < medianCpuCostMap.get(replicaNum)) {
					replicaNum = costMap.getKey();
				}
			}
		}
		return replicaNum;
	}
	
	public String chooseReplica() {
		if (medianCpuCostMap.isEmpty()) {
			return "";
		}
		Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
		String result = gson.toJson(medianCpuCostMap);
		return result;
	}

	public double getData(double data) {
		BigDecimal b = new BigDecimal(data);
		return b.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
}
