package com.sdp.manager;

import org.jboss.netty.channel.MessageEvent;

/**
 * Created by Guoqing on 2016/11/22.
 * The core code of ember server, all the requests are dealt here.
 */
public interface MessageManagerInterface {

    void handleMessage(MessageEvent messageEvent);
}
