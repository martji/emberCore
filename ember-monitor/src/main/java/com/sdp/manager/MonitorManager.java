package com.sdp.manager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sdp.common.EMSGID;
import com.sdp.log.Log;
import com.sdp.messagebody.CtsMsg.nr_cpuStats;
import com.sdp.netty.NetMsg;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author martji
 */
public class MonitorManager extends Thread {

    private final int SLEEP_TIME = 5 * 1000;

    private ConcurrentHashMap<Integer, Channel> serverChannelMap = new ConcurrentHashMap<Integer, Channel>();
    private Map<Integer, Queue<Double>> cpuCostMap = new HashMap<Integer, Queue<Double>>();
    private Map<Integer, Double> medianCpuCostMap = new HashMap<Integer, Double>();

    public MonitorManager() {

    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(SLEEP_TIME);
                if (medianCpuCostMap.size() > 0) {
                    Log.log.info("[CPU] " + medianCpuCostMap.toString());
                }
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
                    Log.log.info("[Netty] server " + serverId + " lose connect");
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
     * @param clientNode
     * @param cpuCost
     */
    public void handle(int clientNode, String cpuCost) {
        Double cost = Double.parseDouble(cpuCost);
        if (cpuCostMap.containsKey(clientNode)) {
            Queue<Double> arrCpuCost = cpuCostMap.get(clientNode);
            arrCpuCost.offer(cost);
            if (arrCpuCost.size() > 10) {
                arrCpuCost.poll();
            }
            medianCpuCostMap.put(clientNode, getData((medianCpuCostMap.get(clientNode) + cost) / 2));
        } else {
            Queue<Double> arrCpuCost = new LinkedList<Double>();
            arrCpuCost.offer(cost);
            cpuCostMap.put(clientNode, arrCpuCost);
            medianCpuCostMap.put(clientNode, getData(cost));
        }
    }

    /**
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
