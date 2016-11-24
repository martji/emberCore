package com.sdp.client;

import java.util.concurrent.Callable;

public class MCThread implements Callable<Boolean> {
	Boolean result = false;
	RMemcachedClientImpl rmClient;
	String key;
	String value;
	
	public  MCThread(RMemcachedClientImpl rmClient, String key, String value) {
		this.rmClient = rmClient;
		this.key = key;
		this.value = value;
	}

	@Override
	public Boolean call() throws Exception {
		result = rmClient.set2M(key, value);
		return result;
	}
	
	
}
