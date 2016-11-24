package com.sdp.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sdp.common.EMSGID;
import com.sdp.messageBody.CtsMsg.nr_connected_mem;
import com.sdp.messageBody.CtsMsg.nr_read_res;
import com.sdp.messageBody.CtsMsg.nr_replicas_res;
import com.sdp.messageBody.CtsMsg.nr_write_res;
import com.sdp.netty.NetMsg;
import com.sdp.operation.BaseOperation;

/**
 * 
 * @author martji
 * 
 */

public class RMemcachedClientImplHandler extends SimpleChannelUpstreamHandler {

	public Stack<String> queue;
	public StringBuffer message;
	public int clientNode;
	public Object lock = new Object();
	public Map<String, NetMsg> requestList;
	ConcurrentMap<String, Vector<Integer>> keyReplicaMap;
	
	Map<String, BaseOperation<?>> opMap;

	public RMemcachedClientImplHandler(int clientNode, StringBuffer message, 
			ConcurrentMap<String, Vector<Integer>> keyReplicaMap) {
		this.clientNode = clientNode;
		this.message = message;
		this.queue = new Stack<String>();
		this.requestList = new HashMap<String, NetMsg>();
		this.keyReplicaMap = keyReplicaMap;
		
		opMap = new HashMap<String, BaseOperation<?>>();
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
		nr_connected_mem.Builder builder = nr_connected_mem.newBuilder();
		NetMsg send = NetMsg.newMessage();
		send.setNodeRoute(clientNode);
		send.setMsgID(EMSGID.nr_connected_mem);
		send.setMessageLite(builder);
		e.getChannel().write(send);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		if (e.getChannel().getLocalAddress() == null) {
			return;
		}
		e.getChannel().close();
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		handle(e);
	}

	private void handle(MessageEvent e) throws InterruptedException {
		NetMsg msg = (NetMsg) e.getMessage();
		switch (msg.getMsgID()) {
		case nr_connected_mem_back: {
			System.out.println("[Netty] connect to server successed, channel: " + e.getChannel());
		}
			break;
		case nr_replicas_res: {
			nr_replicas_res msgBody = msg.getMessageLite();
			String key = msgBody.getKey();
			String value = msgBody.getValue();
			if (key.length() != 0) {
				System.out.println("[Netty] replication update: " + key + ", " + value);
				updateKeyReplicaMap(key, value);
			} else {
				Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
				Map<String, Integer> replicasMap = gson.fromJson(value, 
						new TypeToken<Map<String, Integer>>() {}.getType());
				updateKeyReplicaMap(replicasMap);
			}
		}
			break;
		case nr_read_res: {
			nr_read_res msgBody = msg.getMessageLite();
			String key = msgBody.getKey();
			String value = msgBody.getValue();
			handleReadOp(key, value);
		}
			break;
		case nr_write_res: {
			nr_write_res msgBody = msg.getMessageLite();
			String key = msgBody.getKey();
			String value = msgBody.getValue();
			handleWriteOp(key, value);
		}
			break;
		default:
			break;
		}
	}

	private void handleReadOp(String key, String value) {
		if (opMap.containsKey(key)) {
			@SuppressWarnings("unchecked")
			BaseOperation<String> op = (BaseOperation<String>) opMap.get(key);
			op.getMcallback().gotdata(value);
			opMap.remove(key);
		}
	}
	
	private void handleWriteOp(String key, String value) {
		if (opMap.containsKey(key)) {
			@SuppressWarnings("unchecked")
			BaseOperation<Boolean> op = (BaseOperation<Boolean>) opMap.get(key);
			if (value != null && value.length() > 0) {
				op.getMcallback().gotdata(true);
			} else {
				op.getMcallback().gotdata(false);
			}
			opMap.remove(key);
		}
	}

	private void updateKeyReplicaMap(String key, String value) {
		if (value != null && value.length() > 0) {
			int replicaId = Integer.parseInt(value);
			Vector<Integer> result = decodeReplicasInfo(replicaId);
			if (result != null) {
				if (keyReplicaMap.containsKey(key) && result.size() == 1) {
					keyReplicaMap.remove(key);
					return;
				}
				keyReplicaMap.put(key, result);
			}
		}
	}
	
	private void updateKeyReplicaMap(Map<String, Integer> replicasMap) {
		Set<String> keySet = replicasMap.keySet();
		for (String key : keySet) {
			int replicaId = replicasMap.get(key);
			Vector<Integer> result = decodeReplicasInfo(replicaId);
			if (result != null) {
				if (keyReplicaMap.containsKey(key) && result.size() == 1) {
					keyReplicaMap.remove(key);
					return;
				}
				keyReplicaMap.put(key, result);
			}
		}
	}

	public void addOpMap(String id, BaseOperation<?> op) {
		opMap.put(id, op);
	}
	
	public Vector<Integer> decodeReplicasInfo(int value) {
		Vector<Integer> result = new Vector<Integer>();
		if (value == 0) {
			return null;
		} else {
			while (value > 0) {
				int id = -1;
				int tmp = value;
				while (tmp > 0) {
					tmp /= 2;
					id += 1;
				}
				if (id > -1) {
					result.add(id);
					value -= Math.pow(2, id);
				} else {
					break;
				}
			}
			return result;
		}
	}
}
