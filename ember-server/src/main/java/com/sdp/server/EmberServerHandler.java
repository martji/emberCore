package com.sdp.server;

import com.sdp.log.Log;
import com.sdp.manager.MessageManager;
import org.jboss.netty.channel.*;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author martji
 *         The hander of ember server, which really handles the requests from clients.
 *         This handler is a inner module of netty, all the logical codes are wrote here.
 *         <p>
 *         All the messages ermberServerHandler received are passing down to the messgeManager{@link MessageManager},
 *         and the messageManager will deal with the requests.
 */

public class EmberServerHandler extends SimpleChannelUpstreamHandler {

    private MessageManager messageManager;

    public EmberServerHandler(boolean isDetect) {
        messageManager = new MessageManager();
        if (isDetect) {
            messageManager.startHotSpotDetection();
        }
    }

    /**
     * @param replicasIdMap : the replicasIdMap is shared by all threads.
     * @param mServer       : ember server
     */
    public EmberServerHandler(boolean isDetect, ConcurrentHashMap<String, Vector<Integer>> replicasIdMap,
                              EmberServer mServer) {
        this(isDetect);
        messageManager.initManager(mServer, replicasIdMap);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        handleMessage(e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        Log.log.error(e);
        Channel channel = e.getChannel();
        channel.close();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
    }

    private void handleMessage(MessageEvent e) {
        messageManager.handleMessage(e);
    }
}

