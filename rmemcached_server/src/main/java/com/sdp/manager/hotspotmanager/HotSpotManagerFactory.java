package com.sdp.manager.abstracts;

import com.sdp.manager.CounterHotSpotManager;
import com.sdp.manager.StreamHotSpotManager;

/**
 * Created by Guoqing on 2016/11/22.
 * The hotSpotManager factory, which create the hotSpotManager instance by the type:
 * COUNTER_MODE {@link CounterHotSpotManager} and STREAM_MODE {@link StreamHotSpotManager}.
 *
 * The default mode is STREAM_MODE.
 */
public class HotSpotManagerFactory {

    private static final int COUNTER_MODE = 0;
    private static final int STREAM_MODE = 1;

    public static BaseHotSpotManager createInstance() {
        return createInstance(STREAM_MODE);
    }

    public static BaseHotSpotManager createInstance(int type) {
        switch (type) {
            case COUNTER_MODE:
                return new CounterHotSpotManager();
            case STREAM_MODE:
                return new StreamHotSpotManager();
        }
        return null;
    }
}
