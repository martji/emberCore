package com.sdp.utils;

import java.util.Vector;

/**
 * Created by Guoqing on 2016/12/15.
 */
public class ServerTransUtil {

    public static Vector<Integer> decodeServer(int value) {
        Vector<Integer> result = new Vector<Integer>();
        if (value == 0) {
            return null;
        } else {
            while (value > 0) {
                int id = -1;
                int tmp = value;
                while (tmp > 0) {
                    tmp /= 2;
                    id += 1;
                }
                if (id > -1) {
                    result.add(id);
                    value -= Math.pow(2, id);
                } else {
                    break;
                }
            }
            return result;
        }
    }
}
