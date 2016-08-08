package com.sdp.hotspot.hash;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.sdp.config.GlobalConfigMgr;

/**
 * Created by magq on 16/1/13.
 * address: http://www.partow.net/programming/hashfunctions/index.html#top
 */

public class HashFunction {
	private static int BLOOM_FILTER_LENGTH = 2<<28;
    public static int HASH_FUNCTION_NUMBER = 4;
    private List<String> methodList = new ArrayList<String>();

    public HashFunction() {
        initConfig();
        methodList.add("RSHash");
        methodList.add("JSHash");
        methodList.add("BKDRHash");
        methodList.add("SDBMHash");
        methodList.add("DJBHash");
        methodList.add("DEKHash");
    }

    public void initConfig() {
    	HASH_FUNCTION_NUMBER = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.MULTI_BLOOM_FILTER_NUMBER);
        BLOOM_FILTER_LENGTH = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.BLOOM_FILTER_LENGTH);
    }

    public int[] getHashIndex(String key) {
        int[] result = null;
        if (methodList.size() < HASH_FUNCTION_NUMBER) {
            return result;
        }
        Method method;
        result = new int[HASH_FUNCTION_NUMBER];
        try {
            for (int i = 0; i < HASH_FUNCTION_NUMBER; i++) {
                method = getClass().getMethod(methodList.get(i), String.class);
                long tmp = (Long) method.invoke(this, key);
                result[i] = Math.abs((int) (tmp % BLOOM_FILTER_LENGTH));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public long RSHash(String str) {
        int b = 378551;
        int a = 63689;
        long hash = 0;
        for(int i = 0; i < str.length(); i++) {
            hash = hash * a + str.charAt(i);
            a = a * b;
        }
        return hash;
    }

    public long JSHash(String str) {
        long hash = 1315423911;
        for(int i = 0; i < str.length(); i++) {
            hash ^= ((hash << 5) + str.charAt(i) + (hash >> 2));
        }
        return hash;
    }

    public long BKDRHash(String str) {
        long seed = 131;
        long hash = 0;
        for(int i = 0; i < str.length(); i++) {
            hash = (hash * seed) + str.charAt(i);
        }
        return hash;
    }

    public long SDBMHash(String str) {
        long hash = 0;
        for(int i = 0; i < str.length(); i++) {
            hash = str.charAt(i) + (hash << 6) + (hash << 16) - hash;
        }
        return hash;
    }

    public long DJBHash(String str) {
        long hash = 5381;
        for (int i = 0; i < str.length(); i++) {
            hash = ((hash << 5) + hash) + str.charAt(i);
        }
        return hash;
    }

    public long DEKHash(String str) {
        long hash = str.length();
        for(int i = 0; i < str.length(); i++) {
            hash = ((hash << 5) ^ (hash >> 27)) ^ str.charAt(i);
        }
        return hash;
    }
}
