package com.sdp.common;

import java.lang.reflect.InvocationTargetException;

import com.google.protobuf.GeneratedMessage;
import com.sdp.messageBody.CtsMsg.nr_apply_replica;
import com.sdp.messageBody.CtsMsg.nr_apply_replica_res;
import com.sdp.messageBody.CtsMsg.nr_cpuStats;
import com.sdp.messageBody.CtsMsg.nr_cpuStats_res;
import com.sdp.messageBody.StsMsg.nm_connected;
import com.sdp.messageBody.StsMsg.nm_connected_mem_back;

/**
 * 
 * @author martji
 * 
 */

public class RegisterHandler {
	public static void initHandler() {
		initHandler(EMSGID.nm_connected.ordinal(), nm_connected.class);
		initHandler(EMSGID.nm_connected_mem_back.ordinal(), nm_connected_mem_back.class);
		initHandler(EMSGID.nr_stats.ordinal(), nr_cpuStats.class);
		initHandler(EMSGID.nr_stats_res.ordinal(), nr_cpuStats_res.class);
		initHandler(EMSGID.nr_apply_replica.ordinal(), nr_apply_replica.class);
		initHandler(EMSGID.nr_apply_replica_res.ordinal(), nr_apply_replica_res.class);
	}

	private static void initHandler(int id,
			Class<? extends GeneratedMessage> msgCla) {
		try {
			MessageManager.addMessageCla(id, msgCla);
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
