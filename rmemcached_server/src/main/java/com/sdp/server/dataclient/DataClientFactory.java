package com.sdp.server.dataServer;

import com.sdp.server.DataClient;

/**
 * Created by Guoqing on 2016/11/21.
 * The Factory to create {@link DataClient}.
 *
 * Mode MC_MODE is {@link McDataClient}.
 *
 * The default mode is MC_MODE.
 */
public class DataClientFactory {

    private static final int MC_MODE = 0;
    private static final int REDIS_MODE = 1;

    public static DataClient createDataClient(int id) {
        return createDataClient(MC_MODE, id);
    }

    public static DataClient createDataClient(int clientType, int id) {
        switch (clientType) {
            case MC_MODE:
                return McDataClient.createInstance(id);
            case REDIS_MODE:
                return RedisDataClient.createInstance(id);
        }
        return null;
    }
}