package org.cyy.fw.nedis;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;

public class NedisClientBuilder implements RedisClientBuilder {

	private ServerNode serverNode;
	private int connectTimeoutMills;
	private int eventLoopGroupSize;
	private boolean isTcpNoDelay = true;
	private ByteBufAllocator allocator;
	private RecvByteBufAllocator recvAllocator;
	private MessageSizeEstimator estimator;
	private int connectionPoolSize;
	private int maxConnectionIdleTimeInMills;
	private int minIdleConnection;

	@Override
	public RedisClientBuilder setServerHost(String host) {
		if (serverNode == null) {
			serverNode = new ServerNode();
		}
		serverNode.setHost(host);
		return this;
	}

	@Override
	public RedisClientBuilder setPort(int port) {
		if (serverNode == null) {
			serverNode = new ServerNode();
		}
		serverNode.setPort(port);
		return this;
	}

	public RedisClientBuilder setServerNode(ServerNode server) {
		this.serverNode = server;
		return this;
	}

	@Override
	public RedisClientBuilder setConnectTimeoutMills(int timeout) {
		connectTimeoutMills = timeout;
		return this;
	}

	@Override
	public RedisClientBuilder setEventLoopGroupSize(int eventLoopGroupSize) {
		this.eventLoopGroupSize = eventLoopGroupSize;
		return this;
	}

	@Override
	public NedisClient build() {
		if (serverNode == null) {
			throw new NullPointerException("Must connect a server node.");
		}
		NedisClient client = new NedisClient().setServer(serverNode)
				.setConnectTimeoutMills(connectTimeoutMills)
				.setEventLoopGroupSize(eventLoopGroupSize)
				.setTcpNoDelay(isTcpNoDelay).setByteBufAllocator(allocator)
				.setRecvByteBufAllocator(recvAllocator)
				.setMessageSizeEstimator(estimator)
				.setConnectionPoolSize(connectionPoolSize)
				.setMaxConnectionIdleTimeInMills(maxConnectionIdleTimeInMills)
				.setMinIdleConnections(minIdleConnection);
		client.initialize();
		return client;
	}

	@Override
	public RedisClientBuilder setTcpNoDelay(boolean flag) {
		this.isTcpNoDelay = flag;
		return this;
	}

	@Override
	public RedisClientBuilder setByteBufAllocator(ByteBufAllocator allocator) {
		this.allocator = allocator;
		return this;
	}

	@Override
	public RedisClientBuilder setRecvByteBufAllocator(
			RecvByteBufAllocator recvAllocator) {
		this.recvAllocator = recvAllocator;
		return this;
	}

	@Override
	public RedisClientBuilder setMessageSizeEstimator(
			MessageSizeEstimator estimator) {
		this.estimator = estimator;
		return this;
	}

	@Override
	public RedisClientBuilder setConnectionPoolSize(int poolSize) {
		this.connectionPoolSize = poolSize;
		return this;
	}

	@Override
	public RedisClientBuilder setMaxConnectionIdleTimeInMills(int mills) {
		this.maxConnectionIdleTimeInMills = mills;
		return this;
	}

	@Override
	public RedisClientBuilder setMinIdleConnections(int minIdle) {
		this.minIdleConnection = minIdle;
		return this;
	}

}
