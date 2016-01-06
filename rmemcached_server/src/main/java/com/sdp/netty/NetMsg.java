package com.sdp.netty;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.MessageLite;
import com.sdp.common.EMSGID;
import com.sdp.common.MessageManager;

/**
 * 
 * @author martji
 * 
 */

public class NetMsg {
	EMSGID msgID;
	MessageLite messageLite;
	int nodeRoute;

	private NetMsg() {
	};

	public static NetMsg newMessage() {
		NetMsg msg = new NetMsg();
		msg.setNodeRoute(0);
		return msg;
	}

	NetMsg(byte[] decoded, int id) throws Exception {
		messageLite = MessageManager.getMessage(id, decoded);
	}

	public byte[] getBytes() {
		return messageLite.toByteArray();
	}

	public EMSGID getMsgID() {
		return msgID;
	}

	public void setMsgID(EMSGID id) {
		this.msgID = id;

	}

	@SuppressWarnings("unchecked")
	public <T extends MessageLite> T getMessageLite() {
		return (T) messageLite;
	}

	@SuppressWarnings("rawtypes")
	public void setMessageLite(GeneratedMessage.Builder builder) {
		this.messageLite = builder.build();
	}

	public int getNodeRoute() {
		return nodeRoute;
	}

	public void setNodeRoute(int nodeRoute) {
		this.nodeRoute = nodeRoute;
	}

}
