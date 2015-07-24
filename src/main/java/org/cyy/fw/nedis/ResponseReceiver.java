package org.cyy.fw.nedis;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.timeout.IdleStateEvent;

import java.util.logging.Level;
import java.util.logging.Logger;

@Sharable
public class ResponseReceiver extends SimpleChannelInboundHandler<Object> {

	private static final Logger LOGGER = Logger
			.getLogger(ResponseReceiver.class.getSimpleName());

	private ConnectionPool connectionPool;

	public void setConnectionPool(ConnectionPool connectionPool) {
		this.connectionPool = connectionPool;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg)
			throws Exception {
		ResponeAdapter<Object, Object> responseAdapter = (ResponeAdapter<Object, Object>) ctx
				.channel().attr(ConnectionPool.RESP_ATTR_KEY).get();
		if (responseAdapter != null) {
			if (msg instanceof Throwable) {
				responseAdapter.failed((Throwable) msg);
			} else {
				// responseListener.done((T) process(msg));
				responseAdapter.done(msg);
			}
		}
		closeOrReturnChannel(ctx);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		closeOrReturnChannel(ctx);
		super.exceptionCaught(ctx, cause);
	}

	private void closeOrReturnChannel(ChannelHandlerContext ctx) {
		if (connectionPool != null) {
			connectionPool.returnToPool(ctx.channel());
		} else {
			ctx.close();
		}
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
			throws Exception {
		if (evt instanceof IdleStateEvent) {
			LOGGER.log(Level.WARNING, "remove idle channel: " + ctx.channel());
			if (connectionPool != null) {
				connectionPool.closeIdle(ctx.channel());
			} else {
				ctx.channel().close();
			}
		} else {
			ctx.fireUserEventTriggered(evt);
		}

	}

}
