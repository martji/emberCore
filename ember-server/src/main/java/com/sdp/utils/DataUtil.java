package com.sdp.utils;

import java.text.DecimalFormat;

/**
 * Created by Guoqing on 2016/12/13.
 */
public class DataUtil {

    private static DecimalFormat df = new DecimalFormat("0.000");

    public static String doubleFormat(double value) {
        return df.format(value);
    }
}
