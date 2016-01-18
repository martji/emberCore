package com.sdp.hotspot;

import com.sdp.replicas.ReplicasMgr;

/**
 * Created by magq on 16/1/18.
 */
public abstract class BaseHotspotDetector implements Runnable {

    public BaseHotspotDetector() {}

    public BaseHotspotDetector(ReplicasMgr replicasMgr) {}

    public void handleRegister(String key) {}
}
