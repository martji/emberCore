package com.sdp.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.sdp.common.EMSGID;

/**
 * 
 * @author martji
 * 
 */

public class MDecoder extends FrameDecoder {
	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer) throws Exception {
		if (buffer.readableBytes() < 8) {
			return null;// (1)
		}
		int dataLength = buffer.getInt(buffer.readerIndex());
		if (buffer.readableBytes() < dataLength + 4) {
			return null;// (2)
		}

		buffer.skipBytes(4);// (3)
		int id = buffer.readInt();
		int nodeRoute = buffer.readInt();
		byte[] decoded = new byte[dataLength - 8];

		buffer.readBytes(decoded);
		NetMsg msg = new NetMsg(decoded, id);// (4)
		msg.setMsgID(EMSGID.values()[id]);
		msg.setNodeRoute(nodeRoute);
		return msg;
	}
}