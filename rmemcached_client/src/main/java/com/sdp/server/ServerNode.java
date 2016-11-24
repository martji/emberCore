package com.sdp.server;

public class ServerNode {
	private int id;
	private String host;
	private int rport;
	private int wport;
	private int memcached;
	
	public ServerNode(String host, int rport, int wport, int memcached) {
		this.host = host;
		this.rport = rport;
		this.wport = wport;
		this.memcached = memcached;
	}
	
	public ServerNode(int id, String host, int rport, int wport, int memcached) {
		this.id = id;
		this.host = host;
		this.rport = rport;
		this.wport = wport;
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
	public int getRPort() {
		return rport;
	}
	public void setRPort(int port) {
		this.rport = port;
	}
	public int getWPort() {
		return wport;
	}
	public void setWPort(int port) {
		this.wport = port;
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
