package org.cyy.fw.nedis;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client manager, it manage a thread pool for all client instances.
 * 
 * @author yunyun
 * 
 */
class NedisClientManager {

	private static class InstanceHolder {
		static NedisClientManager instance = new NedisClientManager();
	}

	private static final Logger LOGGER = Logger
			.getLogger(NedisClientManager.class.getName());
	private volatile EventLoopGroup eventLoopGroup;
	// private AtomicInteger clients = new AtomicInteger();
	private Set<NedisClient> clients = new HashSet<>();

	private NedisClientManager() {
		super();
	}

	static NedisClientManager getInstance() {
		return InstanceHolder.instance;
	}

	/**
	 * The event loop group needn't to be initialized more than once, if do
	 * this, only the first initialized action take effect unless the group has
	 * already been shutdown, so does {@link #setEventLoopGroup(EventLoopGroup)}
	 * .
	 * 
	 * @param eventLoopGroupSize
	 */
	synchronized void initEventGroup(int eventLoopGroupSize) {
		// Needn't to be initialized more than once
		if (eventLoopGroup != null) {
			return;
		}
		eventLoopGroup = new NioEventLoopGroup(eventLoopGroupSize);
	}

	/**
	 * 
	 * @param eventLoopGroup
	 * @see #initEventGroup(int)
	 */
	void setEventLoopGroup(EventLoopGroup eventLoopGroup) {
		if (eventLoopGroup != null) {
			return;
		}
		this.eventLoopGroup = eventLoopGroup;
	}

	EventLoopGroup obtainEventGroup() {
		if (eventLoopGroup == null) {
			throw new IllegalStateException(
					"The client has been shutdown, you must call initEventGroup(poolSize) before calling this method.");
		}
		return eventLoopGroup;
	}

	synchronized void registClient(NedisClient client) {
		if (client == null) {
			return;
		}
		// clients.incrementAndGet();
		clients.add(client);
	}

	synchronized void deRegistClient(NedisClient client) {
		if (client == null) {
			return;
		}
		if (!clients.remove(client)) {
			return;
		}
		if (clients.size() == 0 && eventLoopGroup != null) {
			eventLoopGroup.shutdownGracefully();
			eventLoopGroup = null;
			LOGGER.log(Level.INFO, "The event loop group has been shutdown.");
		}
	}

}
