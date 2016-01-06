package com.sdp.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * 
 * @author martji
 * 
 */

public class MEncoder extends OneToOneEncoder {
	@Override
	protected Object encode(ChannelHandlerContext ctx, Channel channel,
			Object msg) throws Exception {
		if (!(msg instanceof NetMsg)) {
			return msg;// (1)
		}
		NetMsg res = (NetMsg) msg;
		byte[] data = res.getBytes();
		int dataLength = data.length + 8;
		ChannelBuffer buf = ChannelBuffers.dynamicBuffer();// (2)
		buf.writeInt(dataLength);
		buf.writeInt(res.msgID.ordinal());
		buf.writeInt(res.getNodeRoute());
		buf.writeBytes(data);
		return buf;// (3)
	}
}