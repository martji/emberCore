package com.sdp.client.others;

import com.sdp.client.ember.EmberDataClient;

import java.util.concurrent.Callable;

public class MCThread implements Callable<Boolean> {
	Boolean result = false;
	EmberDataClient rmClient;
	String key;
	String value;
	
	public  MCThread(EmberDataClient rmClient, String key, String value) {
		this.rmClient = rmClient;
		this.key = key;
		this.value = value;
	}

	public Boolean call() throws Exception {
		result = rmClient.set2DataServer(key, value);
		return result;
	}
}
