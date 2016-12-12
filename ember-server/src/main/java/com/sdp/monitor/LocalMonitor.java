package com.sdp.monitor;

import com.sdp.log.Log;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class LocalMonitor {

    private static LocalMonitor monitor;

    private int dataPort;

    public static LocalMonitor getInstance() {
        if (monitor == null) {
            monitor = new LocalMonitor();
        }
        return monitor;
    }

    private double getCpuCost(int port) {
        double cpuCost = 0.0;
        try {
            String shellCmd = System.getProperty("user.dir") + "/scripts/monitor.sh " + port;
            Process ps = Runtime.getRuntime().exec(shellCmd);
            ps.waitFor();

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            String result;
            result = br.readLine();
            if (result == null || result.length() == 0) {
                return cpuCost;
            }
            String[] paras = result.split("\\s+");
            cpuCost = Double.parseDouble(paras[8]);

            br.close();
        } catch (Exception e) {
            Log.log.warn("[Shell] command execute failed");
        }
        Log.log.info("[CPU] port " + port + " cost = " + cpuCost);
        return cpuCost;
    }

    public void setPort(int port) {
        dataPort = port;
    }

    public Double getCpuCost() {
        return getCpuCost(dataPort);
    }
}