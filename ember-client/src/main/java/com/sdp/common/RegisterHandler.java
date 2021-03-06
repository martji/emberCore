package com.sdp.common;

import com.google.protobuf.GeneratedMessage;
import com.sdp.message.CtsMsg.*;

import java.lang.reflect.InvocationTargetException;

/**
 * @author martji
 */

public class RegisterHandler {
    public static void initHandler() {
        initHandler(EMSGID.nr_connected_mem.ordinal(), nr_connected_mem.class);
        initHandler(EMSGID.nr_connected_mem_back.ordinal(),
                nr_connected_mem_back.class);
        initHandler(EMSGID.nr_read.ordinal(), nr_read.class);
        initHandler(EMSGID.nr_read_res.ordinal(), nr_read_res.class);
        initHandler(EMSGID.nr_register.ordinal(), nr_register.class);
        initHandler(EMSGID.nr_replicas_res.ordinal(), nr_replicas_res.class);
        initHandler(EMSGID.nr_write.ordinal(), nr_write.class);
        initHandler(EMSGID.nr_write_res.ordinal(), nr_write_res.class);
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
