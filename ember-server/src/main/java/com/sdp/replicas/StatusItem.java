package com.sdp.replicas;

/**
 * Created by Guoqing on 2016/12/22.
 */
public class StatusItem implements Comparable<StatusItem> {

    private int serverId;
    private double workload;

    public StatusItem(int serverId, double workload) {
        this.serverId = serverId;
        this.workload = workload;
    }

    @Override
    public int compareTo(StatusItem that) {
        return (int) (this.workload - that.workload);
    }

    public int getServerId() {
        return serverId;
    }

    public double getWorkload() {
        return workload;
    }
}
