package com.sdp.server;

import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.sdp.common.RegisterHandler;
import com.sdp.netty.MDecoder;
import com.sdp.netty.MEncoder;

/**
 * 
 * @author martji
 * 
 */

public class MServer {
	ServerBootstrap bootstrap;
	MServerHandler mServerHandler;
	
	Logger log;
	int port;

	public static void main(String args[]) {
		RegisterHandler.initHandler();
		new MServer();
	}

	public MServer() {
		PropertyConfigurator.configure(System.getProperty("user.dir") + "/config/log4j.properties");
		log = Logger.getLogger( MServer.class.getName());
		getConfig();
		mServerHandler = new MServerHandler();
		init(port);
	}

	public void init(int port) {
		bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory(new MServerPipelineFactory(mServerHandler));
		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", true);
		bootstrap.setOption("reuseAddress", true);
		bootstrap.bind(new InetSocketAddress(port));
		System.out.println("Monitor start service.");
	}
	
	public void getConfig() {
		String configPath = System.getProperty("user.dir") + "/config/config.properties";
		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(configPath));
			port = Integer.parseInt(properties.getProperty("port"));
		} catch (Exception e) {
			log.error("wrong config.properties", e);
		}
	}

	private class MServerPipelineFactory implements ChannelPipelineFactory {

		MServerHandler mServerHandler;
		
		public MServerPipelineFactory(MServerHandler mServerHandler) {
			this.mServerHandler = mServerHandler;
		}

		public ChannelPipeline getPipeline() throws Exception {
			ChannelPipeline pipeline = Channels.pipeline();
			pipeline.addLast("decoder", new MDecoder());
			pipeline.addLast("encoder", new MEncoder());
			pipeline.addLast("handler", mServerHandler);
			return pipeline;
		}
	}
}
