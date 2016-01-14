package com.sdp.hotspot;

/**
 * Created by magq on 16/1/13.
 * address: http://www.partow.net/programming/hashfunctions/index.html#top
 */

public class HashFunction {
    private static HashFunction ourInstance = new HashFunction();

    public static HashFunction getInstance() {
        return ourInstance;
    }

    private HashFunction() {
    }

    public static int[] getHashIndex(String key) {
        return null;
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
