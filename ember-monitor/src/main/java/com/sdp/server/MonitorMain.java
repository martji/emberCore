package com.sdp.server;

import com.sdp.common.RegisterHandler;
import com.sdp.log.Log;

public class MonitorMain {

    /**
     * @param args
     */
    public static void main(String[] args) {
        RegisterHandler.initHandler();
        Log.init();
        new MonitorServer();
    }
}
