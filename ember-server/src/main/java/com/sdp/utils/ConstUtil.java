package com.sdp.utils;

/**
 * Created by Guoqing on 2016/11/28.
 * All default parameters are set here.
 */
public class ConstUtil {

    public static final String DATA_CLIENT_MODE = "0";

    public static final String REPLICA_MODE = "0";
    public static final String UPDATE_STATUS_TIME = "5";
    public static final String HOT_SPOT_BUFFER_SIZE = "1000";

    public static final String IS_DETECT_HOT_SPOT = "false";
    public static final String HOT_SPOT_MANAGER_MODE = "0";
    public static final String SLICE_TIME = "15";

    public static final String HOT_SPOT_THRESHOLD = "100";
    public static final String HOT_SPOT_PERCENTAGE = "0.2";
    public static final String HOT_SPOT_INFLUENCE = "0.1";

    public static final String BLOOM_FILTER_NUMBER = "100";
    public static final String BLOOM_FILTER_LENGTH = "4";
    public static final String FREQUENT_PERCENTAGE = "0.2";

    public static final String COUNTER_NUMBER = "100";
    public static final String TOP_ITEM_NUMBER = "10";

    public static final String FREQUENT_THRESHOLD = "10";

    public static final String INTERVAL = "50";

    public static final String ERROR_RATE = "0.00002";

    public static final double UNBALANCE_THRESHOLD = 2;

    public static final int REPLICA_RS = 2;
    public static final int REPLICA_SPORE = 1;
    public static final int REPLICA_EMBER = 0;

    public static String getReplicaMode(int mode) {
        switch (mode) {
            case REPLICA_EMBER:
                return "Ember";
            case REPLICA_SPORE:
                return "Spore";
            case REPLICA_RS:
                return "RS";
            default:
                return "Other";
        }
    }
}
