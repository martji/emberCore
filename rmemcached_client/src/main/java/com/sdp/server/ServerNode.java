package com.sdp.server;

public class ServerNode {
	private int id;
	private String host;
	private int port;
	private int memcached;
	
	public ServerNode(String host, int port, int memcached) {
		this.host = host;
		this.port = port;
		this.memcached = memcached;
	}
	
	public ServerNode(int id, String host, int port, int memcached) {
		this.id = id;
		this.host = host;
		this.port = port;
		this.memcached = memcached;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public int getMemcached() {
		return memcached;
	}
	public void setMemcached(int memcached) {
		this.memcached = memcached;
	}
	
	public String getServer() {
		return host + ":" + memcached;
	}
}
