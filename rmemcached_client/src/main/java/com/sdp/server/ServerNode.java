package com.sdp.server;

public class ServerNode {
	private int id;
	private String host;
	private int readPort;
	private int writePort;
	private int dataPort;
	
	public ServerNode(String host, int readPort, int writePort, int dataPort) {
		this.host = host;
		this.readPort = readPort;
		this.writePort = writePort;
		this.dataPort = dataPort;
	}
	
	public ServerNode(int id, String host, int readPort, int writePort, int dataPort) {
		this.id = id;
		this.host = host;
		this.readPort = readPort;
		this.writePort = writePort;
		this.dataPort = dataPort;
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
		return readPort;
	}

	public void setRPort(int port) {
		this.readPort = port;
	}

	public int getWPort() {
		return writePort;
	}

	public void setWPort(int port) {
		this.writePort = port;
	}

	public int getDataPort() {
		return dataPort;
	}

	public void setDataPort(int dataPort) {
		this.dataPort = dataPort;
	}

	public String getServer() {
		return host + ":" + dataPort;
	}
}
