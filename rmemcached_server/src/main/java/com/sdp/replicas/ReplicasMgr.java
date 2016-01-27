package com.sdp.replicas;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sdp.common.EMSGID;
import com.sdp.config.GlobalConfigMgr;
import com.sdp.example.Log;
import com.sdp.hotspot.BaseHotspotDetector;
import com.sdp.hotspot.HotspotDetector;
import com.sdp.hotspot.HotspotIdentifier;
import com.sdp.messageBody.CtsMsg.*;
import com.sdp.netty.NetMsg;
import com.sdp.server.MServer;
import com.sdp.server.ServerNode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
/**
 * 
 * @author martji
 *
 */

public class ReplicasMgr implements CallBack {
	BaseHotspotDetector hotspotDetector;
	
	int serverId;
	Map<Integer, ServerNode> serversMap;
	MServer mServer;
	MemcachedClient mc;
	int protocol;
	int replicasMode = 0;
	
	private static int exptime = 60*60*24*10;
	ExecutorService pool = Executors.newCachedThreadPool();
	
	ConcurrentHashMap<Integer, MemcachedClient> spyClientMap = new ConcurrentHashMap<Integer, MemcachedClient>();
	public ConcurrentHashMap<String, Vector<Integer>> replicasIdMap = null;
	ConcurrentHashMap<String, LockKey> LockKeyMap = new ConcurrentHashMap<String, LockKey>();
	ConcurrentHashMap<Integer, Channel> clientChannelMap = new ConcurrentHashMap<Integer, Channel>();
	ConcurrentHashMap<Integer, Integer> hotspotsList = new ConcurrentHashMap<Integer, Integer>();
	
	public ReplicasMgr() {
        this.replicasMode = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.REPLICA_MODE);
		this.replicasIdMap = new ConcurrentHashMap<String, Vector<Integer>>();
		hotspotsList.put(1, 0);
		initHotspotDetector();
	}
	
	public ReplicasMgr(int serverId, Map<Integer, ServerNode> serversMap, MServer mServer, int protocol) {
		this();
		this.serverId = serverId;
		this.serversMap = serversMap;
		this.mServer = mServer;
		this.protocol = protocol;
	}
	
	public ReplicasMgr(MServer mServer, ConcurrentHashMap<String, Vector<Integer>> replicasIdMap) {
		this.serverId = GlobalConfigMgr.id;
		this.serversMap = GlobalConfigMgr.serversMap;
		this.mServer = mServer;
		this.protocol = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.REPLICA_PROTOCOL);
		this.replicasIdMap = replicasIdMap;
		hotspotsList.put(1, 0);
	}
	
	public void initHotspotDetector() {
		if ((Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.HOTSPOT_DETECTOR_MODE)
                == GlobalConfigMgr.DATA_STREAM_MODE) {
            hotspotDetector = new HotspotDetector();
        } else {
            hotspotDetector = new HotspotIdentifier(this.replicasIdMap);
        }
		hotspotDetector.setCallBack(new BaseHotspotDetector.MCallBack() {
			public void dealHotspot() {
				dealHotData();
			}

			public void dealColdspot() {
				dealColdData();
			}

			public void dealHotspot(String key) {
				dealHotData(key);
			}
		});
		new Thread(hotspotDetector).start();
		
		// retire replicas
		if (replicasMode == 1) {
			new Thread(new Runnable() {
				public void run() {
					while (true) {
						try {
							Thread.sleep(30*1000);
						} catch (Exception e) {
							e.printStackTrace();
						}
						if (replicasIdMap != null) {
							System.out.println("[Retire] retire the hotspots!");
							Set<String> hotspots = replicasIdMap.keySet();
							for (String key : hotspots) {
								Vector<Integer> maps = replicasIdMap.get(key);
								if (maps.size() <= 2) {
									replicasIdMap.remove(key);
								} else {
									maps.remove(maps.size() - 1);
								}
							}
						}
					}
				}
			}).start();
		}
	}

	public void initThread() {
		new Thread(new Runnable() {
			public void run() {
				Iterator<Entry<Integer, ServerNode>> iterator = serversMap.entrySet().iterator();
				while (iterator.hasNext()) {
					Entry<Integer, ServerNode> map = iterator.next();
					int id = map.getKey();
					if (id != serverId) {
						MemcachedClient spyClient = buildAMClient(id);
						spyClientMap.put(id, spyClient);
					}
				}
			}
		}).start();
	}
	
	public int getLockState(String key) {
		LockKey lock = LockKeyMap.get(key);
		if (lock == null) {
			return LockKey.unLock;
		}
		return lock.state;
	}

	public void setLockState(String key, Integer state) {
		LockKey lock = LockKeyMap.get(key);
		if (lock != null) {
			lock.state = state;
			LockKeyMap.put(key, lock);
		} else {
			Log.log.error("set Lock state error");
			return;
		}
	}
	
	/**
	 * 
	 * @return true if the pre-lock is null or the pre-lock is not badlock
	 */
	public boolean setLockKey(String key, LockKey lock) {
		LockKey lockKey = LockKeyMap.put(key, lock);	// the previous lock
		if (lockKey != null && lockKey.state != LockKey.badLock) {
			return true;
		}
		return lockKey == null;
	}

	public int desLockKeyCount(String key) {
		LockKey lock = LockKeyMap.get(key);
		if (lock != null) {
			lock.ncount--;
			LockKeyMap.put(key, lock);
			return lock.ncount;
		}
		return 0;
	}

	public boolean removeLock(String key) {
		return LockKeyMap.remove(key) != null;
	}
	
	public boolean getSetState(OperationFuture<Boolean> res) {
		try {
			if (res.get()) {
				return true;
			}
		} catch (Exception e) {}
		return false;
	}

	/**
	 *
	 * @param e
     */
	public void handle(MessageEvent e) {
		NetMsg msg = (NetMsg) e.getMessage();
		switch (msg.getMsgID()) {
		case nr_connected_mem: {
			int clientId = msg.getNodeRoute();
			clientChannelMap.put(clientId, e.getChannel());
			System.out.println("[Netty] server hear channelConnected from client: " + e.getChannel());
			nr_connected_mem_back.Builder builder = nr_connected_mem_back.newBuilder();
			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nr_connected_mem_back);
			e.getChannel().write(send);
		}
			break;
		case nm_connected: {
			System.out.println("[Netty] server hear channelConnected from other server: " + e.getChannel());
		}
			break;
		case nr_register: {
			nr_register msgLite = msg.getMessageLite();
			String key = msgLite.getKey(); // the original key
			handleRegister(e.getChannel(), key);
		}
			break;
		case nr_read: {
			nr_read msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			int failedId = msg.getNodeRoute();
			handleReadFailed(e.getChannel(), key, failedId);
		}
			break;
		case nr_write: {
			handleWrite(e, msg);
		}
			break;
		default:
			break;
		}
	}
	
	/**
	 * @param channel
	 * @param key
	 * @param failedId
	 * return the value and recovery the failed node
	 */
	private void handleReadFailed(Channel channel, String key, int failedId) {
		String oriKey = getOriKey(key);
		if (replicasIdMap.containsKey(oriKey)) {
			String value = null;
			if (failedId == serverId) {
				Vector<Integer> vector = replicasIdMap.get(oriKey);
				MemcachedClient mClient = spyClientMap.get(vector.get(0));
				value = (String) mClient.get(oriKey);
				if (value != null && value.length() > 0) {
					mc.set(oriKey, 3600, value);
				}
				
			} else {
				value = (String) mc.get(oriKey);
				if (value != null && value.length() > 0) {
					MemcachedClient mClient = spyClientMap.get(failedId);
					mClient.set(oriKey, exptime, value);
				}
			}
			nr_read_res.Builder builder = nr_read_res.newBuilder();
			builder.setKey(key);
			builder.setValue(value);
			NetMsg msg = NetMsg.newMessage();
			msg.setMessageLite(builder);
			msg.setMsgID(EMSGID.nr_read_res);
			channel.write(msg);
		}
	}
	
	/**
	 * @param channel
	 * @param key
	 * collect the register info
	 */
	private void handleRegister(Channel channel, String key) {
		hotspotDetector.handleRegister(key);
	}

    public void dealHotData(String key) {
        //TODO
    }

	public void dealHotData() {
		if (LocalSpots.hotspots.keySet().size() > 0) {
			Set<String> hotspots = new HashSet<String>();
			hotspots.addAll(LocalSpots.hotspots.keySet());
			Map<String, Integer> hotitems = new HashMap<String, Integer>();
			if (mServer == null) {
				return;
			}
			String replicasInfo = mServer.getAReplica();
			if (replicasInfo == null || replicasInfo.length() == 0) {
				return;
			}
			List<Map.Entry<Integer, Double>> list = getReplicasInfoMap(replicasInfo);
			Set<String> dealedHotspots = new HashSet<String>();
			for (String key : hotspots) {
				int replicaId = getReplicaId(list, key);
				if (replicaId != -1) {
					boolean result = createReplica(key, replicaId);
					if (result) {
						dealedHotspots.add(key);
						Vector<Integer> vector = null;
						if (!replicasIdMap.containsKey(key)) {
							vector = new Vector<Integer>();
							vector.add(serverId);
							vector.add(replicaId);
							replicasIdMap.put(key, vector);
						} else {
							if (!replicasIdMap.get(key).contains(replicaId)) {
								replicasIdMap.get(key).add(replicaId);
								vector = replicasIdMap.get(key);
							}
						}
						hotitems.put(key, encodeReplicasInfo(vector));
						
						// caculate replicas number
						int localCount = vector.size() - 1;
						if (localCount <= 1) {
							hotspotsList.put(1, hotspotsList.get(1) + 1);
						} else {
							if (hotspotsList.containsKey(localCount - 1)) {
								hotspotsList.put(localCount - 1, hotspotsList.get(localCount - 1) - 1);
								if (!hotspotsList.containsKey(localCount)) {
									hotspotsList.put(localCount, 0);
								}
								hotspotsList.put(localCount, hotspotsList.get(localCount) + 1);
							}
						}
					}
				}
			}
			Log.log.info("[PId: " + Log.id + "] new hotspots: " + dealedHotspots.size() +
					" [create] " + hotspotsList.toString());
			infoAllClient(hotitems);
			LocalSpots.hotspots = new ConcurrentHashMap<String, String>();
		}
	}
	
	public void dealColdData() {
		if (LocalSpots.coldspots.keySet().size() > 0) {
			Set<String> coldspots = new HashSet<String>();
			coldspots.addAll(LocalSpots.coldspots.keySet());
			Map<String, Integer> colditems = new HashMap<String, Integer>();
			for (String key : coldspots) {
				int localCount = replicasIdMap.get(key).size() - 1;
				hotspotsList.put(localCount, hotspotsList.get(localCount) - 1);
				if (localCount - 1 >= 1) {
					hotspotsList.put(localCount - 1, hotspotsList.get(localCount - 1) + 1);
				}
				int replicaId = replicasIdMap.get(key).size() - 1;
				replicasIdMap.get(key).remove(replicaId);
				colditems.put(key, encodeReplicasInfo(replicasIdMap.get(key)));
				if (replicasIdMap.get(key).size() == 1) {
					replicasIdMap.remove(key);
				}
			}
			Log.log.info("[PId: " + Log.id + "] new coldspots: " + coldspots.size() +
					" [retire] " + hotspotsList.toString());
			infoAllClient(colditems);
			LocalSpots.coldspots = new ConcurrentHashMap<String, String>();
		}
	}

    private List<Map.Entry<Integer, Double>> getReplicasInfoMap(String replicasInfo) {
		List<Map.Entry<Integer, Double>> list = null;
		if (replicasInfo == null || replicasInfo.length() == 0) {
			return list;
		}
		Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
		Map<Integer, Double> cpuCostMap = gson.fromJson(replicasInfo, 
				new TypeToken<Map<Integer, Double>>() {}.getType());
		list = new ArrayList<Entry<Integer, Double>>(cpuCostMap.entrySet());
		Collections.sort(list, new Comparator<Entry<Integer, Double>>() {
			public int compare(Entry<Integer, Double> mapping1,
					Entry<Integer, Double> mapping2) {
				return mapping1.getValue().compareTo(mapping2.getValue());
			}
		});
		return list;
	}
	
	private int getReplicaId(List<Map.Entry<Integer, Double>> list, String key) {
		if (replicasMode == 0) {
			return getReplicaIdStrict(list, key);
		} else {
			return getReplicaIdRandomly(list, key);
		}
	}
	
	private int getReplicaIdStrict(List<Map.Entry<Integer, Double>> list, String key) {
		int replicaId = -1;
		HashSet<String> hosts = new HashSet<String>();
		HashSet<Integer> currentReplicas = new HashSet<Integer>();
		if (replicasIdMap.containsKey(key)) {
			currentReplicas = new HashSet<Integer>(replicasIdMap.get(key));
			for (int id : currentReplicas) {
				hosts.add(serversMap.get(id).getHost());
			}
		} else {
			currentReplicas.add(serverId);
			hosts.add(serversMap.get(serverId).getHost());
		}
		
		for (int i = 0; i < list.size(); i++) {
			int tmp = list.get(i).getKey();
			if (!currentReplicas.contains(tmp)) {
				if (!hosts.contains(serversMap.get(tmp).getHost())) {
					return tmp;
				} else if (replicaId == -1) {
					replicaId = tmp;
				}
			}
		}
		if (replicaId != -1 && replicaId != list.size() - 1) {
			Entry<Integer, Double> tmp = list.get(replicaId);
			list.set(replicaId, list.get(list.size() - 1));
			list.set(list.size() - 1, tmp);
		}
		return replicaId;
	}
	
	public int getReplicaIdRandomly(List<Map.Entry<Integer, Double>> list, String key) {
		int replicaId = -1;
		HashSet<Integer> currentReplicas = new HashSet<Integer>();
		if (replicasIdMap.containsKey(key)) {
			currentReplicas = new HashSet<Integer>(replicasIdMap.get(key));
		} else {
			currentReplicas.add(serverId);
		}
		if (currentReplicas.size() == list.size()) {
			return -1;
		}
		
		int len = list.size();
		Random random = new Random();
		int tmp = random.nextInt(len);
		tmp = list.get(tmp).getKey();
		if (!currentReplicas.contains(tmp)) {
			replicaId = tmp;
		}
		
		return replicaId;
	}
	
	private void infoAllClient(Map<String, Integer> colditems) {
		if (colditems == null || colditems.size() == 0) {
			return;
		}
		Collection<Channel> clients = clientChannelMap.values();
		Vector<Channel> tmp = new Vector<Channel>();
		tmp.addAll(clients);
		for (Channel mchannel: tmp) {
			if (!mchannel.isConnected()) {
				clients.remove(mchannel);
			}
		}
		
		Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
		String replicasInfo = gson.toJson(colditems);
		nr_replicas_res.Builder builder = nr_replicas_res.newBuilder();
		builder.setKey("");
		builder.setValue(replicasInfo);
		NetMsg msg = NetMsg.newMessage();
		msg.setMessageLite(builder);
		msg.setMsgID(EMSGID.nr_replicas_res);
		for (Channel mchannel: clients) {
			mchannel.write(msg);
		}
	}

	public int encodeReplicasInfo(Vector<Integer> replicas) {
		int result = 0;
		for (int id : replicas) {
			result += Math.pow(2, id);
		}
		return result;
	}
	
	/**
	 * @param replicaId
	 */
	public boolean createReplica(String key, int replicaId) {
		MemcachedClient replicaClient;
		if (spyClientMap.containsKey(replicaId)) {
			replicaClient = spyClientMap.get(replicaId);
		} else {
			replicaClient = buildAMClient(replicaId);
			if (replicaClient != null) {
				spyClientMap.put(replicaId, replicaClient);
			} else {
				return false;
			}
		}
		
		String value = (String) mc.get(key);
		if (value == null || value.length() == 0) {
			System.out.println("[ERROR] no value fo this key: " + key);
			return false;
		}
		OperationFuture<Boolean> out = replicaClient.set(key, exptime, value);
		try {
			return out.get();
		} catch (Exception e) {}
		return false;
	}

	private MemcachedClient buildAMClient(int replicaId){
		try {
			MemcachedClient replicaClient;
			ServerNode serverNode = serversMap.get(replicaId);
			String host = serverNode.getHost();
			int memcachedPort = serverNode.getMemcached();
			replicaClient = new MemcachedClient(new InetSocketAddress(host, memcachedPort));
			return replicaClient;
		} catch (Exception e) {}
		return null;
	}

	
	private String getOriKey(String key) {
		if (key.contains(":")) {
			return key.substring(key.indexOf(":") + 1);
		}
		return key;
	}

	public void handleWrite(MessageEvent e, NetMsg msg) {
		final nr_write msgLite = msg.getMessageLite();
		int replicasNum = msg.getNodeRoute();
		String key = msgLite.getKey();
		String value = msgLite.getValue();
		String orikey = getOriKey(key);
		
		Vector<Future<Boolean>> resultVector = new Vector<Future<Boolean>>();
		int threshold = 0;
		if (replicasIdMap.containsKey(orikey)) {
			Vector<Integer> replications = replicasIdMap.get(orikey);
			int count = replications.size();
			threshold = getThreshold(count, replicasNum);
			if (threshold > count) {
				threshold = count - 1;
			}
			for (int i = 1; i < count; i++) {
				MemcachedClient mClient = spyClientMap.get(replications.get(i));
				MCThread thread = new MCThread(mClient, key, value);
				Future<Boolean> f = pool.submit(thread);
				resultVector.add(f);
			}
		}
		
		OperationFuture<Boolean> res = mc.set(orikey, exptime, value);
		boolean setState = getSetState(res);
		if (!setState) {
			value = "";
		} else if (threshold > 0){
			int localCount = 0;
			for (Future<Boolean> f : resultVector) {
				try {
					if (f.get()) {
						localCount ++;
					}
					if (localCount >= threshold) {
						break;
					}
				} catch (Exception e2) {}
			}
			if (localCount < threshold) {
				value = "";
			}
		}
		NetMsg response = getWriteResponse(key, value);
		e.getChannel().write(response);
	}
	

	public Integer getThreshold(int count, int replicasNum) {
		if (replicasNum < 0) {
			return count/replicasNum*-1;
		} else {
			return replicasNum;
		}
	}

	private NetMsg getWriteResponse(String key, String value) {
		nr_write_res.Builder builder = nr_write_res.newBuilder();
		builder.setKey(key);
		builder.setValue(value);
		NetMsg send = NetMsg.newMessage();
		send.setMessageLite(builder);
		send.setMsgID(EMSGID.nr_write_res);
		return send;
	}

	public void setMemcachedClient(MemcachedClient mc) {
		this.mc = mc;
	}
	
	public void setMServer(MServer mServer) {
		this.mServer = mServer;
	}

}
