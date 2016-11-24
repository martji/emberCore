package com.sdp.common;

import java.lang.reflect.InvocationTargetException;

import com.google.protobuf.GeneratedMessage;
import com.sdp.message.CtsMsg.nr_apply_replica;
import com.sdp.message.CtsMsg.nr_apply_replica_res;
import com.sdp.message.CtsMsg.nr_connected_mem;
import com.sdp.message.CtsMsg.nr_connected_mem_back;
import com.sdp.message.CtsMsg.nr_cpuStats;
import com.sdp.message.CtsMsg.nr_cpuStats_res;
import com.sdp.message.CtsMsg.nr_read;
import com.sdp.message.CtsMsg.nr_read_res;
import com.sdp.message.CtsMsg.nr_register;
import com.sdp.message.CtsMsg.nr_replicas_res;
import com.sdp.message.CtsMsg.nr_write;
import com.sdp.message.StsMsg.nm_connected;
import com.sdp.message.StsMsg.nm_connected_mem_back;
import com.sdp.message.StsMsg.nm_read;
import com.sdp.message.StsMsg.nm_read_recovery;
import com.sdp.message.StsMsg.nm_write_1;
import com.sdp.message.StsMsg.nm_write_1_res;
import com.sdp.message.StsMsg.nm_write_2;

/**
 * 
 * @author martji
 * 
 */

public class RegisterHandler {
	public static void initHandler() {
		initHandler(EMSGID.nm_connected.ordinal(), nm_connected.class);
		initHandler(EMSGID.nm_connected_mem_back.ordinal(), nm_connected_mem_back.class);
		initHandler(EMSGID.nr_connected_mem.ordinal(), nr_connected_mem.class);
		initHandler(EMSGID.nr_connected_mem_back.ordinal(), nr_connected_mem_back.class);
		initHandler(EMSGID.nr_read.ordinal(), nr_read.class);
		initHandler(EMSGID.nr_read_res.ordinal(), nr_read_res.class);
		initHandler(EMSGID.nr_stats.ordinal(), nr_cpuStats.class);
		initHandler(EMSGID.nr_stats_res.ordinal(), nr_cpuStats_res.class);
		initHandler(EMSGID.nr_apply_replica.ordinal(), nr_apply_replica.class);
		initHandler(EMSGID.nr_apply_replica_res.ordinal(), nr_apply_replica_res.class);
		initHandler(EMSGID.nr_register.ordinal(), nr_register.class);
		initHandler(EMSGID.nr_replicas_res.ordinal(), nr_replicas_res.class);
		initHandler(EMSGID.nm_read.ordinal(), nm_read.class);
		initHandler(EMSGID.nm_read_recovery.ordinal(), nm_read_recovery.class);
		initHandler(EMSGID.nr_write.ordinal(), nr_write.class);
		initHandler(EMSGID.nm_write_1.ordinal(), nm_write_1.class);
		initHandler(EMSGID.nm_write_1_res.ordinal(), nm_write_1_res.class);
		initHandler(EMSGID.nm_write_2.ordinal(), nm_write_2.class);
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
