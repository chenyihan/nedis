package org.cyy.fw.nedis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.cyy.fw.nedis.util.NedisException;

public class ProtocolDecoder extends ReplayingDecoder<Void> {

	private static final Logger LOGGER = Logger.getLogger(NedisClient.class
			.getName());

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in,
			List<Object> out) throws Exception {
		try {
			out.add(RedisProtocol.parseResponse(in));
		} catch (NedisException e) {
			LOGGER.log(Level.WARNING, e.getMessage(), e);
			out.add(e);
		}

	}

}
