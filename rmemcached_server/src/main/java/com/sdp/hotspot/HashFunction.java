package com.sdp.hotspot;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by magq on 16/1/13.
 * address: http://www.partow.net/programming/hashfunctions/index.html#top
 */

public class HashFunction {
    private static HashFunction ourInstance = null;
    private static int HASH_FUNCTION_NUMBER = 4;
    private List<String> methodList = new ArrayList<String>();

    public static void setHashFunctionNumber(int bloomFilterNumber) {
        HASH_FUNCTION_NUMBER = bloomFilterNumber;
    }

    public static HashFunction getInstance() {
        if (ourInstance == null) {
            ourInstance = new HashFunction();
        }
        return ourInstance;
    }

    private HashFunction() {
        methodList.add("RSHash");
        methodList.add("JSHash");
        methodList.add("BKDRHash");
        methodList.add("SDBMHash");
        methodList.add("DJBHash");
        methodList.add("DEKHash");
    }

    public int[] getHashIndex(String key) {
        int[] result = null;
        if (ourInstance.methodList.size() < HASH_FUNCTION_NUMBER) {
            return result;
        }
        Method method;
        result = new int[HASH_FUNCTION_NUMBER];
        try {
            for (int i = 0; i < HASH_FUNCTION_NUMBER; i++) {
                method = ourInstance.getClass().getMethod(ourInstance.methodList.get(i), String.class);
                long tmp = (Long) method.invoke(ourInstance, key);
                result[i] = (int) tmp;
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
