package com.sdp.replicas;

import java.util.concurrent.Callable;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

public class MCThread implements Callable<Boolean> {
	private static int exptime = 60*60*24*10;
	OperationFuture<Boolean> result;
	MemcachedClient mClient;
	String key;
	String value;
	
	public  MCThread(MemcachedClient mClient, String key, String value) {
		this.mClient = mClient;
		this.key = key;
		this.value = value;
	}

	public Boolean call() throws Exception {
		result = mClient.set(key, exptime, value);
		return result.get();
	}
}
