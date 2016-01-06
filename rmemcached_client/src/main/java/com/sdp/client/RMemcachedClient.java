package com.sdp.client;
/**
 * 
 * @author martji
 *
 */

public interface RMemcachedClient {
	
	public void init();
	public void shutdown();
	
	public String get(String key);
	public boolean set(String key, String value);
	public boolean delete(String key);
}
