package org.cyy.fw.nedis;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;

public interface ClientConfig<T extends ClientConfig<T>> {

	/**
	 * Set connect timeout millseconds
	 * 
	 * @param timeout
	 * @return
	 */
	T setConnectTimeoutMills(int timeout);

	/**
	 * 
	 * Set thread pool size
	 * 
	 * @param poolSize
	 * @return
	 */
	T setEventLoopGroupSize(int poolSize);

	/**
	 * Set TCP_NO_DELAY flag
	 * 
	 * @param flag
	 * @return
	 */
	T setTcpNoDelay(boolean flag);

	/**
	 * Set bytebuf allocator
	 * 
	 * @param allocator
	 * @return
	 */
	T setByteBufAllocator(ByteBufAllocator allocator);

	/**
	 * Set bytebuf allocator
	 * 
	 * @param recvAllocator
	 * @return
	 */
	T setRecvByteBufAllocator(RecvByteBufAllocator recvAllocator);

	/**
	 * Set a Estimator Responsible to estimate size of a message
	 * 
	 * @param estimator
	 * @return
	 */
	T setMessageSizeEstimator(MessageSizeEstimator estimator);

	/**
	 * Connection pool size
	 * 
	 * @param poolSize
	 * @return
	 */
	T setConnectionPoolSize(int poolSize);

	/**
	 * Max idle time of connection in the connection pool
	 * 
	 * @param mills
	 * @return
	 */
	T setMaxConnectionIdleTimeInMills(int mills);

	/**
	 * Min Idle Connections
	 * 
	 * @param minIdle
	 * @return
	 */
	T setMinIdleConnections(int minIdle);

}
