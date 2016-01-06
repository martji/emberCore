package com.sdp.operation;

import com.sdp.future.MCallback;

public class BaseOperation<V> {
	String key = null;
	String value = null;
	int result = 0;
	MCallback<V> mcallback = null;
	
	public BaseOperation() {
		
	}
	
	public BaseOperation(MCallback<V> callback) {
		this.mcallback = callback;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getResult() {
		return result;
	}

	public void setResult(int result) {
		this.result = result;
	}

	public MCallback<V> getMcallback() {
		return mcallback;
	}

	public void setMcallback(MCallback<V> mcallback) {
		this.mcallback = mcallback;
	}
}
