package com.sdp.server;

import com.sdp.common.RegisterHandler;
import com.sdp.log.Log;
import com.sdp.netty.MDecoder;
import com.sdp.netty.MEncoder;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * @author martji
 */

public class MonitorServer {
    ServerBootstrap bootstrap;
    MonitorServerHandler mServerHandler;

    int port;

    public static void main(String args[]) {
        RegisterHandler.initHandler();
        new MonitorServer();
    }

    public MonitorServer() {
        getConfig();
        mServerHandler = new MonitorServerHandler();
        init(port);
    }

    public void init(int port) {
        bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new MServerPipelineFactory(mServerHandler));
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.bind(new InetSocketAddress(port));
        Log.log.info("[Netty] MonitorServer start");
    }

    public void getConfig() {
        String configPath = System.getProperty("user.dir") + "/config/config.properties";
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(configPath));
            port = Integer.parseInt(properties.getProperty("port"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class MServerPipelineFactory implements ChannelPipelineFactory {

        MonitorServerHandler mServerHandler;

        public MServerPipelineFactory(MonitorServerHandler mServerHandler) {
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
