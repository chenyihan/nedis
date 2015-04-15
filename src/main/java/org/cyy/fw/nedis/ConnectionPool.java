package org.cyy.fw.nedis;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.cyy.fw.nedis.util.NedisException;

public class ConnectionPool {

	private final class ChannelCloseFutureListener implements
			ChannelFutureListener {
		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			Channel ch = future.channel();
			LOGGER.log(Level.SEVERE,
					ch + " closed, exception: " + future.cause());
			if (getIdleClosingFlag(ch)) {
				closingIdleChannels.getAndDecrement();
				setIdleClosingFlag(ch, false);
			}
			removeIdleChannel(ch, future.cause());
		}
	}

	public static final class ConnectionPoolConfig {
		private static final int DEFAULT_CONNECTION_POOL_SIZE = 200;
		private int connectTimeoutMills = DEFAULT_CONNECTION_POOL_SIZE;
		private int maxTotal;
		private int maxIdle;
		private int minIdle;
		private int maxIdleTimeInMills;
		private boolean isTcpNoDelay = true;
		private ByteBufAllocator allocator;
		private RecvByteBufAllocator recvAllocator;
		private MessageSizeEstimator estimator;

		public int getConnectTimeoutMills() {
			return connectTimeoutMills;
		}

		public ConnectionPoolConfig setConnectTimeoutMills(
				int connectTimeoutMills) {
			this.connectTimeoutMills = connectTimeoutMills;
			return this;
		}

		public int getMaxTotal() {
			return maxTotal;
		}

		public ConnectionPoolConfig setMaxTotal(int maxTotal) {
			this.maxTotal = maxTotal;
			if (this.maxTotal <= 0) {
				this.maxTotal = DEFAULT_CONNECTION_POOL_SIZE;
			}
			return this;
		}

		public int getMaxIdle() {
			return maxIdle;
		}

		public ConnectionPoolConfig setMaxIdle(int maxIdle) {
			this.maxIdle = maxIdle;
			return this;
		}

		public int getMinIdle() {
			return minIdle;
		}

		public ConnectionPoolConfig setMinIdle(int minIdle) {
			this.minIdle = minIdle;
			return this;
		}

		public int getMaxIdleTimeInMills() {
			return maxIdleTimeInMills;
		}

		public ConnectionPoolConfig setMaxIdleTimeInMills(int maxIdleTimeInMills) {
			this.maxIdleTimeInMills = maxIdleTimeInMills;
			return this;
		}

		public ConnectionPoolConfig setTcpNoDelay(boolean flag) {
			this.isTcpNoDelay = flag;
			return this;
		}

		public ConnectionPoolConfig setByteBufAllocator(
				ByteBufAllocator allocator) {
			this.allocator = allocator;
			return this;
		}

		public ConnectionPoolConfig setRecvByteBufAllocator(
				RecvByteBufAllocator recvAllocator) {
			this.recvAllocator = recvAllocator;
			return this;
		}

		public ConnectionPoolConfig setMessageSizeEstimator(
				MessageSizeEstimator estimator) {
			this.estimator = estimator;
			return this;
		}
	}

	// private static final String RESP_CALLBACK = "resp_callback";
	public static final AttributeKey<ResponeAdapter<?, ?>> RESP_ATTR_KEY = AttributeKey
			.valueOf("resp_callback");
	private static final AttributeKey<Boolean> IDLE_CLOSING_FLAG = AttributeKey
			.valueOf("idle_closing_flag");
	private static final Logger LOGGER = Logger.getLogger(ConnectionPool.class
			.getSimpleName());
	// private static final StringEncoder ENCODER = new StringEncoder();
	private final BlockingQueue<Channel> idleChannels;
	private final Object lock = new Object();
	private EventLoopGroup group;
	private final Bootstrap clientBootstrap;
	private volatile boolean isClosed = true;
	private ServerNode server;
	private int connectTimeoutMills;

	private boolean isTcpNoDelay = true;
	private ByteBufAllocator allocator;
	private RecvByteBufAllocator recvAllocator;
	private MessageSizeEstimator estimator;

	private int maxIdleTimeInMilliSecondes;
	private int maxTotal;
	// private int maxIdle;
	private int minIdle;

	private Semaphore poolSizeController;
	private ChannelFutureListener channelCloseFutureListener = new ChannelCloseFutureListener();
	private AtomicInteger closingIdleChannels;

	public ConnectionPool(EventLoopGroup group, ServerNode server,
			ConnectionPoolConfig config) {
		idleChannels = new LinkedBlockingQueue<>();
		this.connectTimeoutMills = config.getConnectTimeoutMills();
		this.server = server;
		// this.maxIdle = config.getMaxIdle();
		this.maxTotal = config.getMaxTotal();
		this.minIdle = config.getMinIdle();
		this.maxIdleTimeInMilliSecondes = config.getMaxIdleTimeInMills();
		this.isTcpNoDelay = config.isTcpNoDelay;
		this.allocator = config.allocator;
		this.recvAllocator = config.recvAllocator;
		this.estimator = config.estimator;

		this.group = group;
		this.clientBootstrap = new Bootstrap();
		closingIdleChannels = new AtomicInteger();

	}

	public void init() {
		if (!isClosed) {
			LOGGER.log(
					Level.WARNING,
					"The connection pool has already been initialized, it needn't to be initialized again until closed.");
			return;
		}
		synchronized (lock) {
			if (!isClosed) {
				LOGGER.log(
						Level.WARNING,
						"The connection pool has already been initialized, it needn't to be initialized again until closed.");
				return;
			}
			initClientBootstrap();
			initPool();
			isClosed = false;
			// isInit = true;
		}
	}

	private void initClientBootstrap() {
		final ResponseReceiver commandHandler = new ResponseReceiver();
		commandHandler.setConnectionPool(this);
		clientBootstrap
				.group(group)
				.channel(NioSocketChannel.class)
				.option(ChannelOption.SO_KEEPALIVE, true)
				.option(ChannelOption.TCP_NODELAY, isTcpNoDelay)
				.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
						connectTimeoutMills)
				.option(ChannelOption.ALLOCATOR, allocator)
				.option(ChannelOption.RCVBUF_ALLOCATOR, recvAllocator)
				.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR, estimator)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel ch) throws Exception {
						ch.pipeline().addLast(
								LoggingHandler.class.getSimpleName(),
								new LoggingHandler(LogLevel.INFO));
						ch.pipeline().addLast(
								ProtocolDecoder.class.getSimpleName(),
								new ProtocolDecoder());
						// ch.pipeline().addLast(
						// ENCODER.getClass().getSimpleName(), ENCODER);
						ch.pipeline().addLast(
								IdleStateHandler.class.getSimpleName(),
								new IdleStateHandler(0, 0,
										maxIdleTimeInMilliSecondes,
										TimeUnit.MILLISECONDS));
						ch.pipeline().addLast(
								commandHandler.getClass().getSimpleName(),
								commandHandler);
					}
				});
	}

	private void initPool() {
		idleChannels.clear();
		poolSizeController = new Semaphore(maxTotal);
		closingIdleChannels.set(0);
		if (minIdle <= 0) {
			return;
		}
		for (int i = 0; i < minIdle; i++) {
			ChannelFuture connectFuture = createNewChannel();
			if (connectFuture != null) {
				connectFuture.addListener(new ChannelFutureListener() {

					@Override
					public void operationComplete(ChannelFuture future)
							throws Exception {
						if (future.isSuccess()) {
							future.channel().closeFuture()
									.addListener(channelCloseFutureListener);
							addIdleChannel(future.channel());

						} else {
							LOGGER.log(Level.SEVERE,
									future.channel()
											+ " connect failed, exception: "
											+ future.cause());
							poolSizeController.release();
						}
					}
				});
			}
		}
	}

	public int idleSize() {
		return idleChannels.size();
	}

	public <S, T> void sendCommand(BinaryCommand message,
			ResponeAdapter<S, T> responseAdapter) throws InterruptedException {
		if (isClosed) {
			throw new IllegalStateException(
					"The connection pool has not been initialized yet, must call init() to initialize.");
		}
		if (idleSize() < minIdle
				&& sendCommandWithNewChannel(message, responseAdapter)) {
			return;
		}
		if (sendCommandWithPoolChannel(message, responseAdapter)) {
			return;
		}
		if (sendCommandWithNewChannel(message, responseAdapter))
			return;
		throw new NedisException("Connect failed.");
	}

	private <S, T> boolean sendCommandWithPoolChannel(BinaryCommand message,
			ResponeAdapter<S, T> responseAdapter) throws InterruptedException {
		Channel channel = obtainFromPool();
		if (channel != null) {
			sendMessage(channel, message, responseAdapter);
			return true;
		}
		return false;
	}

	private <S, T> boolean sendCommandWithNewChannel(
			final BinaryCommand message,
			final ResponeAdapter<S, T> responseAdapter) {
		ChannelFuture connectFuture = createNewChannel();
		if (connectFuture != null) {
			connectFuture.addListener(new ChannelFutureListener() {

				@Override
				public void operationComplete(ChannelFuture future)
						throws Exception {
					if (future.isSuccess()) {
						future.channel().closeFuture()
								.addListener(channelCloseFutureListener);
						sendMessage(future.channel(), message, responseAdapter);
					} else {
						LOGGER.log(Level.SEVERE,
								future.channel()
										+ " connect failed, exception: "
										+ future.cause());
						poolSizeController.release();
						if (responseAdapter != null) {
							responseAdapter.failed(future.cause());
						}
					}
				}
			});
			return true;
		}
		return false;
	}

	private ChannelFuture createNewChannel() {
		ChannelFuture connectFuture = null;
		if (poolSizeController.tryAcquire()) {
			try {
				connectFuture = clientBootstrap.connect(server.getHost(),
						server.getPort());
			} catch (Throwable e) {
				LOGGER.log(Level.SEVERE, "connect failed", e);
				poolSizeController.release();
			}
		}
		return connectFuture;
	}

	private <S, T> void sendMessage(Channel ch, final BinaryCommand message,
			final ResponeAdapter<S, T> responseAdapter) {
		// LOGGER.log(Level.INFO,
		// "channel:" + ch + ",send command:" + message.toString());
		ch.attr(RESP_ATTR_KEY).set(responseAdapter);
		ByteBuf msg = RedisProtocol.generateRequest(message, ch.alloc());
		ch.writeAndFlush(msg).addListener(
				ChannelFutureListener.CLOSE_ON_FAILURE);
	}

	private Channel obtainFromPool() throws InterruptedException {
		Channel channel = idleChannels.poll();
		while (channel != null && !channel.isActive()) {
			channel = idleChannels.poll();
		}
		if (poolSizeController.availablePermits() != 0) {
			return null;
		}
		if (channel == null || !channel.isActive()) {
			channel = idleChannels.poll(connectTimeoutMills,
					TimeUnit.MILLISECONDS);
			if (channel == null || !channel.isActive()) {
				LOGGER.log(Level.WARNING, "obtain channel from pool timeout");
				return null;
			}
		}
		LOGGER.log(Level.INFO, channel + " reuse");
		return channel;
	}

	public void returnToPool(Channel ch) {
		if (ch != null && ch.isActive()) {
			ch.attr(RESP_ATTR_KEY).set(null);
			if (isClosed) {
				LOGGER.log(Level.INFO,
						"The pool has already been closed, so close this channel directly.");
				try {
					ch.close().sync();
				} catch (InterruptedException e) {
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
				return;
			}
			addIdleChannel(ch);
		}
	}

	public ChannelFuture closeIdle(Channel ch) {
		LOGGER.log(Level.INFO, "idleSize:" + idleSize());
		int idleSize = idleSize();
		if ((idleSize = idleSize - closingIdleChannels.getAndIncrement()) <= minIdle) {
			// restore the increment operation
			closingIdleChannels.getAndDecrement();
			LOGGER.log(Level.WARNING, "The pool must keep " + minIdle
					+ " idle connections but the idle size is " + idleSize
					+ ", so this channel will not be closed now.");
			return null;
		}
		setIdleClosingFlag(ch, true);
		return ch.close();
	}

	public void close() throws InterruptedException {
		if (isClosed) {
			LOGGER.log(Level.WARNING,
					"The connection pool has already been closed or has not initialized yet.");
			return;
		}
		synchronized (lock) {
			if (isClosed) {
				return;
			}
			// isInit = false;
			close0();
			isClosed = true;
		}
	}

	private void close0() throws InterruptedException {
		LOGGER.log(Level.INFO, "Close this pool.");
		ChannelGroup channelGroup = new DefaultChannelGroup(
				GlobalEventExecutor.INSTANCE);
		Channel ch = idleChannels.poll();
		while (ch != null) {
			try {
				channelGroup.add(ch);
				idleChannels.remove(ch);
			} catch (VirtualMachineError e) {
				throw e;
			} catch (Throwable t) {
				// NOOP, swallow this exception
				LOGGER.log(Level.WARNING,
						"Swallow this exception:" + t.getMessage());
			}
			ch = idleChannels.poll();
		}
		channelGroup.close().sync();
	}

	private void addIdleChannel(Channel ch) {
		if (idleChannels.offer(ch)) {
			LOGGER.log(Level.INFO, ch + " has been returned");
		}
		LOGGER.log(Level.INFO, "idleSize:" + idleSize());
	}

	private void removeIdleChannel(Channel ch, Throwable cause) {
		if (cause != null) {
			LOGGER.log(Level.WARNING, cause.getMessage(), cause);
		}
		idleChannels.remove(ch);
		poolSizeController.release();
		LOGGER.log(Level.INFO, "idleSize:" + idleSize());
	}

	private boolean getIdleClosingFlag(Channel ch) {
		Boolean flag = ch.attr(IDLE_CLOSING_FLAG).get();
		return flag == null ? false : flag;
	}

	private void setIdleClosingFlag(Channel ch, boolean flag) {
		ch.attr(IDLE_CLOSING_FLAG).set(flag);
	}
}
