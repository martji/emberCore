package com.sdp.manager.hotspotmanager;

import com.sdp.config.ConfigManager;

/**
 * Created by Guoqing on 2016/11/22.
 * The hotSpotManager factory, which create the hotSpotManager instance by the type:
 * COUNTER_MODE {@link CounterHotSpotManager} and STREAM_MODE {@link StreamHotSpotManager}.
 * <p>
 * The default mode is STREAM_MODE.
 */
public class HotSpotManagerFactory {

    private static final int STREAM_MODE = 0;
    private static final int COUNTER_MODE = 1;
    private static final int TOP_K_MODE = 2;
    private static final int MULTI_BLOOM_MODE = 3;
    private static final int COUNTER_BLOOM_MODE = 4;
    private static final int FREQUENT_MODE = 5;

    public static BaseHotSpotManager createInstance() {
        int type = (Integer) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_MANAGER_MODE);
        return createInstance(type);
    }

    public static BaseHotSpotManager createInstance(int type) {
        switch (type) {
            case COUNTER_MODE:
                return new CounterHotSpotManager();
            case STREAM_MODE:
                return new StreamHotSpotManager();
            case TOP_K_MODE:
                return new TopKHotSpotManager();
            case MULTI_BLOOM_MODE:
                return new MultiBloomHotSpotManager();
            case COUNTER_BLOOM_MODE:
                return new CounterBloomHotSpotManager();
            case FREQUENT_MODE:
                return new FrequentHotSpotManager();
        }
        return null;
    }
}
