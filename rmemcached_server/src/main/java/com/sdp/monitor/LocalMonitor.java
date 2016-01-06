package com.sdp.monitor;
import java.io.BufferedReader;  
import java.io.InputStreamReader;  

import org.jboss.netty.channel.Channel;
  
public class LocalMonitor {  
	static LocalMonitor monitor = null;
	int memcachedPort;
	Channel monitorChannel;
	public double cpuCost = 0.0;
	
	public static void main(String[] args) {
		LocalMonitor.getInstance().getCpuCost(20000);
	}
	
	public static LocalMonitor getInstance() {
		if (monitor == null) {
			monitor = new LocalMonitor();
		}
		return monitor;
	}
	
    public double getCpuCost(int port){  
    	double cpuCost = 0.0;
        try {  
            String shcmd = System.getProperty("user.dir") + "/scripts/monitor.sh " + port;
            Process ps = Runtime.getRuntime().exec(shcmd);  
            ps.waitFor(); 

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));  
            String result;  
            result = br.readLine();  
            if (result == null || result.length() == 0) {
                System.out.println("no result!"); 
                return cpuCost;
            }
            String[] paras = result.split("\\s+");
            cpuCost = Double.parseDouble(paras[8]);
            
            br.close();
            monitor.cpuCost = cpuCost;
        }   
        catch (Exception e) {  
//        	Log.log.error("getCpucost", e);
        }  
        return cpuCost;
    }

	public void setPort(int memcached) {
		monitor.memcachedPort = memcached;
	}
	
	public int getPort() {
		return monitor.memcachedPort;
	}

	public Double getCpuCost() {
		return getCpuCost(monitor.memcachedPort);
	}

	public void setMonitorChannel(Channel getmChannel) {
		monitor.monitorChannel = getmChannel;
	}
}