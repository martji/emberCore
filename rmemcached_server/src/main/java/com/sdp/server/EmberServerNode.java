package com.sdp.server;

/**
 * @author magq
 * Ember server node information, which includes the server id, host address and three different port.
 *
 * The data port is the port of back data setrver(redis, memcached and etc.).
 */

public class EmberServerNode {

	private int id;
	private String host;
	private int readPort;
	private int writePort;
	private int dataPort;

	public EmberServerNode(int id, String host, int readPort, int writePort, int dataPort) {
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

	public int getReadPort() {
		return readPort;
	}

	public void setReadPort(int readPort) {
		this.readPort = readPort;
	}

	public int getWritePort() {
		return writePort;
	}

	public void setWritePort(int writePort) {
		this.writePort = writePort;
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
