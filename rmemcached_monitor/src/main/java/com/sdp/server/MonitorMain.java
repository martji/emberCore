package com.sdp.server;

import com.sdp.common.RegisterHandler;

public class MonitorMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RegisterHandler.initHandler();
		new MServer();
	}
}
