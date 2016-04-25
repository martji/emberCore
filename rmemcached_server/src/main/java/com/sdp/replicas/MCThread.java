package com.sdp.replicas;

import com.sdp.db.DBClientInterface;
import net.spy.memcached.internal.OperationFuture;

import java.util.concurrent.Callable;

public class MCThread implements Callable<Boolean> {
	private static int exptime = 60*60*24*10;
	OperationFuture<Boolean> result;
	DBClientInterface mClient;
	String key;
	String value;
	
	public  MCThread(DBClientInterface mClient, String key, String value) {
		this.mClient = mClient;
		this.key = key;
		this.value = value;
	}

	public Boolean call() throws Exception {
		return mClient.set(key, value);
	}
}
