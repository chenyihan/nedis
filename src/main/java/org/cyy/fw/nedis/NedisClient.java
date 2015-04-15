package org.cyy.fw.nedis;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.cyy.fw.nedis.ConnectionPool.ConnectionPoolConfig;
import org.cyy.fw.nedis.util.TextEncoder;

/**
 * A redis client, this client makes use of Netty for network communication
 * framework, this client is named Nedis due to Netty+Redis. All of command
 * request of this client are asynchronously, your program needn't to wait for
 * the request to finish, when the request is finished, the framework will
 * invoke the {@link ResponseCallback} instead, if the command has been
 * processed by the server successfully, {@link ResponseCallback#done(Object)}
 * will be called, otherwise, {@link ResponseCallback#failed(Throwable)} will
 * called, The Response Callback is a generic type, each command may generate a
 * difference type, such as SET command will return a string type result, DEL
 * command will return a long integer type result and EXIST command will return
 * a boolean type result so on.
 * <p>
 * A client instance can be used to connection one server only, if you have more
 * than one server instances, you have to resort to some client instances more,
 * all of these client instances share one thread pool(EventLoopGroup) and are
 * managed by {@link NedisClientManager}, the client have to register itself to
 * the manager, the manager provides a thread pool for all clients, and it will
 * be shutdown after all clients are shutdown.
 * <p>
 * This client use a connection pool, the command can reuse the connections to
 * prevent establishing a connection to the server frequently, due to the heavy
 * performance overhead. You can set the pool size by calling
 * {@link #setConnectionPoolSize(int)}, see {@link ConnectionPool} for more
 * details of the connection pool.
 * <p>
 * You must call the {@link #initialize()} before send any command by this
 * client, otherwise the client will not work and throws a Exception. The
 * framework suggest you use the {@link NedisClientBuilder#build()} to create
 * the client instance, it will call {@link #initialize()} for you. you can
 * create the client instance follow the sample code:
 * 
 * <pre>
 * String host = &quot;192.168.1.107&quot;;
 * int port = 6379;
 * client = new NedisClientBuilder().setServerHost(host).setPort(port)
 * 		.setConnectTimeoutMills(connectTimeoutMills).setConnectionPoolSize(5)
 * 		.build();
 * </pre>
 * 
 * See {@link NedisClientBuilder} for more parameters, and send command by
 * client follow the below sample code:
 * 
 * <pre>
 * 
 * 
 * client.flushAll(null);
 * Thread.sleep(CMD_PAUSE_TIME);
 * client.set(null, &quot;key1&quot;, &quot;value1&quot;);
 * ResponseCallback&lt;String&gt; respCallback = new ResponseCallback&lt;String&gt;() {
 * 
 * 	&#064;Override
 * 	public void done(String result) {
 * 		assertEquals(&quot;value1&quot;, result);
 * 	}
 * 
 * 	&#064;Override
 * 	public void failed(Throwable cause) {
 * 		fail(cause);
 * 	}
 * };
 * Thread.sleep(CMD_PAUSE_TIME);
 * client.get(respCallback, &quot;key1&quot;);
 * 
 * respCallback = new ResponseCallback&lt;String&gt;() {
 * 
 * 	&#064;Override
 * 	public void done(String result) {
 * 		assertEquals(&quot;null&quot;, result);
 * 		controller.countDown();
 * 	}
 * 
 * 	&#064;Override
 * 	public void failed(Throwable cause) {
 * 		fail(cause);
 * 		controller.countDown();
 * 	}
 * };
 * 
 * client.get(respCallback, &quot;key2&quot;);
 * 
 * 
 * </pre>
 * 
 * The framework also support key sharding, If your system have several server
 * instance and want to distribute keys to all servers evenly, the
 * {@link ShardedNedis} will help you, see {@link ShardedNedis} for more
 * details.
 * 
 * @author yunyun
 * @see NedisClientBuilder
 * @see ShardedNedis
 * 
 */
public class NedisClient implements ClientConfig<NedisClient> {

	private static final Logger LOGGER = Logger.getLogger(NedisClient.class
			.getName());
	private ServerNode server;
	private int connectTimeoutMills;
	private int eventLoopGroupSize;
	private boolean isTcpNoDelay = true;
	private ByteBufAllocator allocator;
	private RecvByteBufAllocator recvAllocator;
	private MessageSizeEstimator estimator;
	private volatile boolean isInit;
	private ConnectionPool connectionPool;
	private int connectionPoolSize;
	private int maxConnectionIdleTimeInMills;
	private int minIdleConnection;

	NedisClient() {
		super();
	}

	/**
	 * Initialize the client, this method must be called before sending any
	 * command by this client.
	 */
	public synchronized void initialize() {
		if (isInit) {
			LOGGER.log(Level.INFO, "The client has already been initialized.");
			return;
		}
		NedisClientManager.getInstance().initEventGroup(eventLoopGroupSize);
		NedisClientManager.getInstance().registClient(this);
		ConnectionPoolConfig config = new ConnectionPoolConfig()
				.setByteBufAllocator(allocator)
				.setConnectTimeoutMills(connectTimeoutMills)
				.setMaxTotal(connectionPoolSize)
				.setMessageSizeEstimator(estimator)
				.setRecvByteBufAllocator(recvAllocator)
				.setTcpNoDelay(isTcpNoDelay)
				.setMaxIdleTimeInMills(maxConnectionIdleTimeInMills)
				.setMinIdle(minIdleConnection);
		connectionPool = new ConnectionPool(NedisClientManager.getInstance()
				.obtainEventGroup(), server, config);
		connectionPool.init();
		isInit = true;
	}

	NedisClient setServer(ServerNode server) {
		this.server = server;
		return this;
	}

	public NedisClient setConnectTimeoutMills(int timeout) {
		this.connectTimeoutMills = timeout;
		return this;
	}

	public NedisClient setEventLoopGroupSize(int eventLoopGroupSize) {
		this.eventLoopGroupSize = eventLoopGroupSize;
		return this;
	}

	@Override
	public NedisClient setTcpNoDelay(boolean flag) {
		this.isTcpNoDelay = flag;
		return this;
	}

	@Override
	public NedisClient setByteBufAllocator(ByteBufAllocator allocator) {
		this.allocator = allocator;
		return this;
	}

	@Override
	public NedisClient setRecvByteBufAllocator(
			RecvByteBufAllocator recvAllocator) {
		this.recvAllocator = recvAllocator;
		return this;
	}

	@Override
	public NedisClient setMessageSizeEstimator(MessageSizeEstimator estimator) {
		this.estimator = estimator;
		return this;
	}

	@Override
	public NedisClient setConnectionPoolSize(int poolSize) {
		this.connectionPoolSize = poolSize;
		return this;
	}

	/**
	 * Shutdown this client, this client will be removed from the manager and
	 * the connection pool will be closed.
	 */
	public synchronized void shutdown() {
		if (!isInit) {
			LOGGER.log(
					Level.WARNING,
					"The client has not been initialized correctly, you must call initialize() before using this client");
			return;
		}
		try {
			connectionPool.close();
		} catch (InterruptedException e) {
			LOGGER.log(Level.WARNING, e.getMessage(), e);
		}
		NedisClientManager.getInstance().deRegistClient(this);
		isInit = false;
	}

	/**
	 * Serialize the specified key and return the serialized value, the returned
	 * serialized value can be deserialized by 'restore' command, see
	 * {@link #restore(ResponseCallback, String, byte[])} for more details.
	 * 
	 * @param respCallBack
	 *            The response callback, when the request has been done
	 *            successfully, the serialized value will be pass the
	 *            {@link ResponseCallback#done(Object)}, the returned serialized
	 *            value's type is byte[]
	 * @param key
	 *            the specified key
	 * @see #restore(ResponseCallback, String, byte[])
	 * @see #restore(ResponseCallback, String, long, byte[], boolean)
	 */
	public void dump(ResponseCallback<byte[]> respCallBack, String key) {
		sendCommandWitByteResponseAdapter(RedisCommand.DUMP, respCallBack, key);
	}

	/**
	 * Set survival seconds of the specified key, if the key is expired, it will
	 * be removed automatically, if the specified key already exist, value true
	 * will be passed to {@link ResponseCallback#done(Object)} when the command
	 * has been completed, otherwise value false will be passed instead,
	 * {@link ResponseCallback#failed(Throwable)} will be called when the
	 * command is failed by some reason.
	 * 
	 * @param respCallBack
	 *            The response callback
	 * 
	 * @param key
	 *            the specified key
	 * @param seconds
	 *            the survival seconds of the key
	 */
	public void expire(ResponseCallback<Boolean> respCallBack, String key,
			long seconds) {
		sendCommandWithBoolResponseAdapter(RedisCommand.EXPIRE, respCallBack,
				key, String.valueOf(seconds));
	}

	/**
	 * Set survival time of the specified key, unlike
	 * {@link #expire(ResponseCallback, String, long)}, this method set a UNIX
	 * timestamp
	 * 
	 * @param respCallBack
	 *            The response callback
	 * @param key
	 *            the specified key
	 * @param ts
	 *            the expire timestamp
	 * @see #expire(ResponseCallback, String, long)
	 */
	public void expireAt(ResponseCallback<Boolean> respCallBack, String key,
			long ts) {
		sendCommandWithBoolResponseAdapter(RedisCommand.EXPIREAT, respCallBack,
				key, String.valueOf(ts));
	}

	/**
	 * Migrate the specified key for current instance to the destination DB, the
	 * operation is atomic, the key will be added to the destination and removed
	 * from current instance and pass 'OK' to
	 * {@link ResponseCallback#done(Object)} when successfully.
	 * 
	 * @param respCallBack
	 *            The response callback
	 * @param key
	 *            the specified key
	 * @param destHost
	 *            destination host
	 * @param destPort
	 *            destination port
	 * @param destDb
	 *            destination DB
	 * @param timeout
	 *            timeout
	 */
	public void migrate(ResponseCallback<String> respCallBack, String key,
			String destHost, int destPort, int destDb, long timeout) {
		sendCommandWithStringResponseAdapter(RedisCommand.MIGRATE,
				respCallBack, destHost, String.valueOf(destPort), key,
				String.valueOf(destDb), String.valueOf(timeout));
	}

	/**
	 * Move the key from current DB to the destination DB
	 * 
	 * @param respCallBack
	 *            The response callback, true will be passed to done method if
	 *            moved successfully, otherwise false will be passed instead
	 * @param key
	 *            the specified key
	 * @param destDb
	 *            destination DB
	 */
	public void move(ResponseCallback<Boolean> respCallBack, String key,
			int destDb) {
		sendCommandWithBoolResponseAdapter(RedisCommand.MOVE, respCallBack,
				key, String.valueOf(destDb));
	}

	/**
	 * Query the reference times of the specified key
	 * 
	 * @param respCallBack
	 *            The response callback, the times will be passed to the done
	 *            method
	 * @param key
	 *            the specified key
	 */
	public void objectRefcount(ResponseCallback<Long> respCallBack, String key) {
		sendCommandWithLongResponseAdapter(RedisCommand.OBJECT, respCallBack,
				RedisKeyword.REFCOUNT.getText(), key);
	}

	/**
	 * Query the idle time of the specified key
	 * 
	 * @param respCallBack
	 *            The response callback, the idle time of this key will be
	 *            passed to the done method
	 * @param key
	 *            the specified key
	 */
	public void objectIdletime(ResponseCallback<Long> respCallBack, String key) {
		sendCommandWithLongResponseAdapter(RedisCommand.OBJECT, respCallBack,
				RedisKeyword.IDLETIME.getText(), key);
	}

	/**
	 * Query the encoding of the specified key
	 * 
	 * @param respCallBack
	 *            The response callback, the encoding string will be passed to
	 *            the done method, the encoding string contains: raw, int,
	 *            ziplist, linkedlist, intset, hashtable, skiplist
	 * @param key
	 *            the specified key
	 */
	public void objectEncoding(ResponseCallback<String> respCallBack, String key) {
		sendCommandWithStringResponseAdapter(RedisCommand.OBJECT, respCallBack,
				RedisKeyword.ENCODING.getText(), key);
	}

	/**
	 * Remove the expire time of the specified key
	 * 
	 * @param respCallBack
	 *            The response callback, true will be passed when remove
	 *            successfully
	 * @param key
	 *            the specified key
	 */
	public void persist(ResponseCallback<Boolean> respCallBack, String key) {
		sendCommandWithBoolResponseAdapter(RedisCommand.PERSIST, respCallBack,
				key);
	}

	/**
	 * Set survival milliseconds of the specified key, unlike
	 * {@link #expire(ResponseCallback, String, long)}, this method set
	 * milliseconds not seconds.
	 * 
	 * @param respCallBack
	 * @param key
	 * @param millSeconds
	 * @see #expire(ResponseCallback, String, long)
	 */
	public void pExpire(ResponseCallback<Boolean> respCallBack, String key,
			long millSeconds) {
		sendCommandWithBoolResponseAdapter(RedisCommand.PEXPIRE, respCallBack,
				key, String.valueOf(millSeconds));
	}

	/**
	 * Set survival time of the specified key, it set milliseconds UNIX
	 * timestamp while {@link #expireAt(ResponseCallback, String, long)} set the
	 * seconds UNIX timestamp
	 * 
	 * @param respCallBack
	 * @param key
	 * @param millTs
	 * @see #expireAt(ResponseCallback, String, long)
	 */
	public void pExpireAt(ResponseCallback<Boolean> respCallBack, String key,
			long millTs) {
		sendCommandWithBoolResponseAdapter(RedisCommand.PEXPIREAT,
				respCallBack, key, String.valueOf(millTs));
	}

	/**
	 * Query the milliseconds of time to live of the specified key
	 * 
	 * @param respCallBack
	 *            The response callback, milliseconds of time to live will be
	 *            passed to done method
	 * @param key
	 *            the specified key
	 */
	public void pTTL(ResponseCallback<Long> respCallBack, String key) {
		sendCommandWithLongResponseAdapter(RedisCommand.PTTL, respCallBack, key);
	}

	/**
	 * Return a random key from current DB.
	 * 
	 * @param respCallBack
	 *            The response callback, the random key will be passed to done
	 *            method.
	 */
	public void randomKey(ResponseCallback<String> respCallBack) {
		sendCommandWithStringResponseAdapter(RedisCommand.RANDOMKEY,
				respCallBack);
	}

	/**
	 * Rename the specified key
	 * 
	 * @param respCallBack
	 *            The response callback, 'OK' will be passed to done method.
	 * @param key
	 *            old key name
	 * @param newkey
	 *            new key name
	 */
	public void rename(ResponseCallback<String> respCallBack, String key,
			String newkey) {
		sendCommandWithStringResponseAdapter(RedisCommand.RENAME, respCallBack,
				key, newkey);
	}

	/**
	 * Rename the specified key only when the new key does not exist.
	 * 
	 * @param respCallBack
	 *            The response callback, true will be passed to done method if
	 *            rename successfully, if the new key already exist, false will
	 *            be passed
	 * @param key
	 *            old key name
	 * @param newkey
	 *            new key name
	 */
	public void renameNX(ResponseCallback<Boolean> respCallBack, String key,
			String newkey) {
		sendCommandWithBoolResponseAdapter(RedisCommand.RENAMENX, respCallBack,
				key, newkey);
	}

	/**
	 * Deserialized the serialized value which has been serialized by
	 * {@link #dump(ResponseCallback, String)} to the specified key
	 * 
	 * @param respCallBack
	 *            The response callback, 'OK' will be passed to done method if
	 *            restore successfully
	 * @param key
	 * @param serialValue
	 *            the serialized value
	 */
	public void restore(ResponseCallback<String> respCallBack, String key,
			byte[] serialValue) {
		restore(respCallBack, key, 0, serialValue, false);
	}

	/**
	 * Deserialized the serialized value which has been serialized by
	 * {@link #dump(ResponseCallback, String)} to the specified key and set the
	 * expire time.
	 * 
	 * @param respCallBack
	 *            The response callback, 'OK' will be passed to done method if
	 *            restore successfully
	 * @param key
	 *            the specified key
	 * @param millTTL
	 *            expire time, milliseconds
	 * @param serialValue
	 *            the serialized value
	 * @param replace
	 *            whether replace when the key already exist
	 */
	public void restore(ResponseCallback<String> respCallBack, String key,
			long millTTL, byte[] serialValue, boolean replace) {
		if (replace) {
			sendCommandWithStringResponseAdapter0(RedisCommand.RESTORE,
					respCallBack, TextEncoder.encode(key),
					TextEncoder.encode(String.valueOf(millTTL)), serialValue,
					RedisKeyword.REPLACE.getBinary());
		} else {
			sendCommandWithStringResponseAdapter0(RedisCommand.RESTORE,
					respCallBack, TextEncoder.encode(key),
					TextEncoder.encode(String.valueOf(millTTL)), serialValue);
		}
	}

	/**
	 * Sort the the specified key's value
	 * 
	 * @param respCallBack
	 *            The response callback, all value after sorted will be passed
	 *            to done method
	 * @param key
	 *            the specified key
	 */
	public void sort(ResponseCallback<String[]> respCallBack, String key) {
		sort(respCallBack, key, (String) null);
	}

	/**
	 * Sort the the specified key's value, and store the sorted result to the
	 * destination key.
	 * 
	 * @param respCallBack
	 *            The response callback, all value after sorted will be passed
	 *            to done method
	 * @param key
	 *            the specified key
	 * @param destKey
	 *            the destination key
	 */
	public void sort(ResponseCallback<String[]> respCallBack, String key,
			String destKey) {
		sort(respCallBack, key, null, destKey);

	}

	/**
	 * Sort the the specified key's value, with some extra parameters
	 * 
	 * @param respCallBack
	 *            The response callback, all value after sorted will be passed
	 *            to done method
	 * @param key
	 *            the specified key
	 * @param params
	 *            extra parameters
	 */
	public void sort(ResponseCallback<String[]> respCallBack, String key,
			SortingParams params) {
		sort(respCallBack, key, params, null);
	}

	/**
	 * Sort the the specified key's value, with some extra parameters and store
	 * the sorted result to the destination key.
	 * 
	 * @param respCallBack
	 *            The response callback, all value after sorted will be passed
	 *            to done method
	 * @param key
	 *            the specified key
	 * @param params
	 *            extra parameters
	 * @param destKey
	 *            the destination key
	 */
	public void sort(ResponseCallback<String[]> respCallBack, String key,
			SortingParams params, String destKey) {
		List<String> args = new ArrayList<>();
		args.add(key);
		if (params != null) {
			args.addAll(params.getParams());
		}
		if (destKey != null) {
			args.add(RedisKeyword.STORE.getText());
			args.add(destKey);
		}
		sendCommandWithStringArrayResponseAdapter(RedisCommand.SORT,
				respCallBack, args.toArray(new String[0]));
	}

	/**
	 * Query the seconds of time to live of the specified key
	 * 
	 * @param respCallBack
	 *            The response callback, seconds of time to live will be passed
	 *            to done method
	 * @param key
	 *            the specified key
	 */
	public void ttl(ResponseCallback<Long> respCallBack, String key) {
		sendCommandWithLongResponseAdapter(RedisCommand.TTL, respCallBack, key);
	}

	/**
	 * Query the type of the specified key's value
	 * 
	 * @param respCallBack
	 *            The response callback, the type string will be passed to done
	 *            method, the type string contains: none, string, list, set
	 *            zset, hash
	 * @param key
	 */
	public void type(ResponseCallback<String> respCallBack, String key) {
		sendCommandWithStringResponseAdapter(RedisCommand.TYPE, respCallBack,
				key);
	}

	/**
	 * Scan elements of the specified key, the result will be wrapped in
	 * {@link ScanResult}, which contains the cursor and the result list
	 * 
	 * @param respCallBack
	 *            The response callback
	 * @param cursor
	 *            start cursor
	 */
	public void scan(ResponseCallback<ScanResult<String>> respCallBack,
			String cursor) {
		scan(respCallBack, cursor, null);
	}

	/**
	 * Scan elements of the specified key with more parameters
	 * 
	 * @param respCallBack
	 *            The response callback
	 * @param cursor
	 *            start cursor
	 * @param params
	 *            more parameters
	 */
	public void scan(ResponseCallback<ScanResult<String>> respCallBack,
			String cursor, ScanParams params) {
		List<String> args = new ArrayList<>();
		args.add(cursor);
		if (params != null) {
			args.addAll(params.getParams());
		}
		sendCommandWithStringScanResultResponseAdapter(RedisCommand.SCAN,
				respCallBack, args.toArray(new String[0]));
	}

	/**
	 * Set a value to the specified key
	 * 
	 * @param respCallBack
	 *            The response callback, OK will be passed to done
	 * @param key
	 *            the specified key
	 * @param value
	 *            the specified value
	 */
	public void set(ResponseCallback<String> respCallBack, String key,
			String value) {
		sendCommandWithStringResponseAdapter(RedisCommand.SET, respCallBack,
				key, value);
	}

	/**
	 * Get the value of the specified key
	 * 
	 * @param respCallBack
	 *            The response callback, the returned value will be passed to
	 *            done
	 * @param key
	 *            the specified key
	 */
	public void get(ResponseCallback<String> respCallBack, String key) {
		sendCommandWithStringResponseAdapter(RedisCommand.GET, respCallBack,
				key);
	}

	/**
	 * Delete one or more keys
	 * 
	 * @param respCallBack
	 *            The response callback, the deleted numbers will be passed to
	 *            done
	 * @param key
	 *            one key
	 * @param moreKeys
	 *            more keys
	 */
	public void del(ResponseCallback<Long> respCallBack, String key,
			String... moreKeys) {
		String[] args = CmdArgumentTool.combineArgs(key, moreKeys);
		sendCommandWithLongResponseAdapter(RedisCommand.DEL, respCallBack, args);
	}

	/**
	 * Query all keys which conform to the pattern
	 * 
	 * @param respCallBack
	 *            The response callback, the conformed keys will be passed to
	 *            done
	 * @param pattern
	 *            pattern
	 */
	public void keys(ResponseCallback<String[]> respCallBack, String pattern) {
		sendCommandWithStringArrayResponseAdapter(RedisCommand.KEYS,
				respCallBack, pattern);
	}

	/**
	 * Query whether the specified key exists or not
	 * 
	 * @param respCallBack
	 *            The response callback, true will be passed when exist,
	 *            otherwise false will be passed to done
	 * @param key
	 */
	public void exists(ResponseCallback<Boolean> respCallBack, String key) {
		sendCommandWithBoolResponseAdapter(RedisCommand.EXISTS, respCallBack,
				key);
	}

	/**
	 * Append a value to the specified key's value, if the key does not exist,
	 * just create the key and set the value to the key
	 * 
	 * @param respCallBack
	 *            The response callback, the final value of the key will be
	 *            passed to done method
	 * @param key
	 *            the specified key
	 * @param value
	 *            the appended value
	 */
	public void append(ResponseCallback<Long> respCallBack, String key,
			String value) {
		sendCommandWithLongResponseAdapter(RedisCommand.APPEND, respCallBack,
				key, value);
	}

	/**
	 * Set or clear the bit of the specified offset of the specified key
	 * 
	 * @param respCallBack
	 *            The response callback, the original value of the bit will
	 *            passed to done
	 * @param key
	 *            the specified key
	 * @param offset
	 *            the specified offset
	 * @param value
	 *            the bit value
	 */
	public void setBit(ResponseCallback<Long> respCallBack, String key,
			long offset, int value) {
		sendCommandWithLongResponseAdapter(RedisCommand.SETBIT, respCallBack,
				key, String.valueOf(offset), String.valueOf(value));
	}

	/**
	 * Get the specified offset bit value
	 * 
	 * @param respCallBack
	 *            The response callback, the bit value will be passed to done
	 *            method
	 * @param key
	 *            the specified key
	 * @param offset
	 *            the specified offset
	 */
	public void getBit(ResponseCallback<Long> respCallBack, String key,
			long offset) {
		sendCommandWithLongResponseAdapter(RedisCommand.GETBIT, respCallBack,
				key, String.valueOf(offset));
	}

	/**
	 * Query the number of 1 bit of the key's value
	 * 
	 * @param respCallBack
	 *            The response callback, the number will be passed to done
	 * @param key
	 *            the specified key
	 */
	public void bitCount(ResponseCallback<Long> respCallBack, String key) {
		sendCommandWithLongResponseAdapter(RedisCommand.BITCOUNT, respCallBack,
				key);
	}

	/**
	 * Query the number of 1 bit of the key's value, within the specified range
	 * 
	 * @param respCallBack
	 *            The response callback, the number will be passed to done
	 * @param key
	 *            the specified key
	 * @param start
	 *            the start bit
	 * @param end
	 *            the end bit, both start and end parameter can be a negative
	 *            number, if so, it indicate the offset of the last bit, such
	 *            as, -1 is the last bit, -2 is the last second bit and so one
	 */
	public void bitCount(ResponseCallback<Long> respCallBack, String key,
			long start, long end) {
		sendCommandWithLongResponseAdapter(RedisCommand.BITCOUNT, respCallBack,
				key, String.valueOf(start), String.valueOf(end));
	}

	/**
	 * Bit operation within one or more bits and store the result to the
	 * destination key, the operation includes: AND, OR, XOR, NOT.
	 * 
	 * @param respCallBack
	 *            The response callback
	 * @param bitOP
	 *            Bit operation
	 * @param destKey
	 *            the destination key
	 * @param srcKey
	 *            operated key
	 * @param srcKeys
	 *            another more operated keys
	 */
	public void bitop(ResponseCallback<Long> respCallBack, BitOP bitOP,
			String destKey, String srcKey, String... srcKeys) {
		String[] args = CmdArgumentTool.combineArgs(new String[] {
				bitOP.name(), destKey, srcKey }, srcKeys);
		sendCommandWithLongResponseAdapter(RedisCommand.BITOP, respCallBack,
				args);
	}

	/**
	 * Increase 1 to the specified key
	 * 
	 * @param respCallBack
	 *            The response callback
	 * @param key
	 *            the specified key, the value after increased will be passed to
	 *            done
	 */
	public void incr(ResponseCallback<Long> respCallBack, String key) {
		sendCommandWithLongResponseAdapter(RedisCommand.INCR, respCallBack, key);
	}

	public void incrBy(ResponseCallback<Long> respCallBack, String key,
			long increment) {
		sendCommandWithLongResponseAdapter(RedisCommand.INCRBY, respCallBack,
				key, String.valueOf(increment));
	}

	public void incrByFloat(ResponseCallback<Double> respCallBack, String key,
			double increment) {
		sendCommandWithDoubleResponseAdapter(RedisCommand.INCRBYFLOAT,
				respCallBack, key, String.valueOf(increment));
	}

	public void decr(ResponseCallback<Long> respCallBack, String key) {
		sendCommandWithLongResponseAdapter(RedisCommand.DECR, respCallBack, key);
	}

	public void decrBy(ResponseCallback<Long> respCallBack, String key,
			long increment) {
		sendCommandWithLongResponseAdapter(RedisCommand.DECRBY, respCallBack,
				key, String.valueOf(increment));
	}

	public void getRange(ResponseCallback<String> respCallBack, String key,
			long start, long end) {
		sendCommandWithStringResponseAdapter(RedisCommand.GETRANGE,
				respCallBack, key, String.valueOf(start), String.valueOf(end));
	}

	public void getSet(ResponseCallback<String> respCallBack, String key,
			String value) {
		sendCommandWithStringResponseAdapter(RedisCommand.GETSET, respCallBack,
				key, value);
	}

	public void mSet(ResponseCallback<String> respCallBack, String key,
			String value) {
		mSet(respCallBack, new KeyValuePair(key, value));
	}

	public void mSet(ResponseCallback<String> respCallBack,
			KeyValuePair keyValue, KeyValuePair... moreKeyValues) {
		String[] args = CmdArgumentTool.combineArgs(keyValue, moreKeyValues);
		sendCommandWithStringResponseAdapter(RedisCommand.MSET, respCallBack,
				args);
	}

	public void mGet(ResponseCallback<String[]> respCallBack, String key,
			String... moreKeys) {
		String[] args = CmdArgumentTool.combineArgs(key, moreKeys);
		sendCommandWithStringArrayResponseAdapter(RedisCommand.MGET,
				respCallBack, args);
	}

	public void mSetNX(ResponseCallback<Boolean> respCallBack, String key,
			String value) {
		mSetNX(respCallBack, new KeyValuePair(key, value));
	}

	public void mSetNX(ResponseCallback<Boolean> respCallBack,
			KeyValuePair keyValue, KeyValuePair... moreKeyValues) {
		String[] args = CmdArgumentTool.combineArgs(keyValue, moreKeyValues);
		sendCommandWithBoolResponseAdapter(RedisCommand.MSETNX, respCallBack,
				args);
	}

	public void setNX(ResponseCallback<Boolean> respCallBack, String key,
			String value) {
		sendCommandWithBoolResponseAdapter(RedisCommand.SETNX, respCallBack,
				key, value);
	}

	public void setEX(ResponseCallback<String> respCallBack, String key,
			long seconds, String value) {
		sendCommandWithStringResponseAdapter(RedisCommand.SETEX, respCallBack,
				key, String.valueOf(seconds), value);
	}

	public void pSetEX(ResponseCallback<String> respCallBack, String key,
			long millSeconds, String value) {
		sendCommandWithStringResponseAdapter(RedisCommand.PSETEX, respCallBack,
				key, String.valueOf(millSeconds), value);
	}

	public void setRange(ResponseCallback<Long> respCallBack, String key,
			long offset, String value) {
		sendCommandWithLongResponseAdapter(RedisCommand.SETRANGE, respCallBack,
				key, String.valueOf(offset), value);
	}

	public void strLen(ResponseCallback<Long> respCallBack, String key) {
		sendCommandWithLongResponseAdapter(RedisCommand.STRLEN, respCallBack,
				key);
	}

	public void hSet(ResponseCallback<Boolean> respCallBack, String key,
			String field, String value) {
		sendCommandWithBoolResponseAdapter(RedisCommand.HSET, respCallBack,
				key, field, value);
	}

	public void hSetNX(ResponseCallback<Boolean> respCallBack, String key,
			String field, String value) {
		sendCommandWithBoolResponseAdapter(RedisCommand.HSETNX, respCallBack,
				key, field, value);
	}

	public void hGet(ResponseCallback<String> respCallBack, String key,
			String field) {
		sendCommandWithStringResponseAdapter(RedisCommand.HGET, respCallBack,
				key, field);
	}

	public void hGetAll(ResponseCallback<Map<String, String>> respCallBack,
			String key) {
		sendCommandWithHashMapResponseAdapter(RedisCommand.HGETALL,
				respCallBack, key);
	}

	public void hDel(ResponseCallback<Long> respCallBack, String key,
			String field) {
		sendCommandWithLongResponseAdapter(RedisCommand.HDEL, respCallBack,
				key, field);
	}

	public void hExist(ResponseCallback<Boolean> respCallBack, String key,
			String field) {
		sendCommandWithBoolResponseAdapter(RedisCommand.HEXISTS, respCallBack,
				key, field);
	}

	public void hIncrBy(ResponseCallback<Long> respCallBack, String key,
			String field, long increment) {
		sendCommandWithLongResponseAdapter(RedisCommand.HINCRBY, respCallBack,
				key, field, String.valueOf(increment));
	}

	public void hIncrByFloat(ResponseCallback<Double> respCallBack, String key,
			String field, double increment) {
		sendCommandWithDoubleResponseAdapter(RedisCommand.HINCRBYFLOAT,
				respCallBack, key, field, String.valueOf(increment));
	}

	public void hKeys(ResponseCallback<String[]> respCallBack, String key) {
		sendCommandWithStringArrayResponseAdapter(RedisCommand.HKEYS,
				respCallBack, key);
	}

	public void hVals(ResponseCallback<String[]> respCallBack, String key) {
		sendCommandWithStringArrayResponseAdapter(RedisCommand.HVALS,
				respCallBack, key);
	}

	public void hLen(ResponseCallback<Long> respCallBack, String key) {
		sendCommandWithLongResponseAdapter(RedisCommand.HLEN, respCallBack, key);
	}

	public void hMSet(ResponseCallback<String> respCallBack, String key,
			String field, String value) {
		hMSet(respCallBack, key, new KeyValuePair(field, value));
	}

	public void hMSet(ResponseCallback<String> respCallBack, String key,
			KeyValuePair pair, KeyValuePair... moreKeyValuePairs) {
		String[] args = CmdArgumentTool.combineArgs(pair, moreKeyValuePairs);
		args = CmdArgumentTool.combineArgs(key, args);
		sendCommandWithStringResponseAdapter(RedisCommand.HMSET, respCallBack,
				args);
	}

	public void hMGet(ResponseCallback<String[]> respCallBack, String key,
			String field, String... moreFields) {
		String[] args = CmdArgumentTool.combineArgs(
				new String[] { key, field }, moreFields);
		sendCommandWithStringArrayResponseAdapter(RedisCommand.HMGET,
				respCallBack, args);
	}

	public void hScan(
			ResponseCallback<ScanResult<Entry<String, String>>> respCallBack,
			String key, String cursor) {
		hScan(respCallBack, key, cursor, null);
	}

	public void hScan(
			ResponseCallback<ScanResult<Entry<String, String>>> respCallBack,
			String key, String cursor, ScanParams scanParams) {
		List<String> args = new ArrayList<>();
		args.add(key);
		args.add(cursor);
		if (scanParams != null) {
			args.addAll(scanParams.getParams());
		}
		sendCommandWithHashScanResultResponseAdapter(RedisCommand.HSCAN,
				respCallBack, args.toArray(new String[0]));
	}

	public void lPush(ResponseCallback<Long> respCallBack, String key,
			String value, String... moreValue) {
		String[] args = CmdArgumentTool.combineArgs(
				new String[] { key, value }, moreValue);
		sendCommandWithLongResponseAdapter(RedisCommand.LPUSH, respCallBack,
				args);
	}

	public void lPushX(ResponseCallback<Long> respCallBack, String key,
			String value) {
		sendCommandWithLongResponseAdapter(RedisCommand.LPUSHX, respCallBack,
				key, value);
	}

	public void lPop(ResponseCallback<String> respCallBack, String key) {
		sendCommandWithStringResponseAdapter(RedisCommand.LPOP, respCallBack,
				key);
	}

	public void rPush(ResponseCallback<Long> respCallBack, String key,
			String value, String... moreValues) {
		String[] args = CmdArgumentTool.combineArgs(
				new String[] { key, value }, moreValues);
		sendCommandWithLongResponseAdapter(RedisCommand.RPUSH, respCallBack,
				args);
	}

	public void rPushX(ResponseCallback<Long> respCallBack, String key,
			String value, String... moreValues) {
		String[] args = CmdArgumentTool.combineArgs(
				new String[] { key, value }, moreValues);
		sendCommandWithLongResponseAdapter(RedisCommand.RPUSHX, respCallBack,
				args);
	}

	public void rPop(ResponseCallback<String> respCallBack, String key) {
		sendCommandWithStringResponseAdapter(RedisCommand.RPOP, respCallBack,
				key);
	}

	public void blPop(ResponseCallback<String[]> respCallBack, long timeout,
			String key, String... moreKeys) {
		String[] args = CmdArgumentTool.combineArgs(key, moreKeys);
		args = CmdArgumentTool.combineArgs(args,
				new String[] { String.valueOf(timeout) });
		sendCommandWithStringArrayResponseAdapter(RedisCommand.BLPOP,
				respCallBack, args);
	}

	public void brPop(ResponseCallback<String[]> respCallBack, long timeout,
			String key, String... moreKeys) {
		String[] args = CmdArgumentTool.combineArgs(key, moreKeys);
		args = CmdArgumentTool.combineArgs(args,
				new String[] { String.valueOf(timeout) });
		sendCommandWithStringArrayResponseAdapter(RedisCommand.BRPOP,
				respCallBack, args);
	}

	public void lRange(ResponseCallback<String[]> respCallBack, String key,
			long start, long end) {
		sendCommandWithStringArrayResponseAdapter(RedisCommand.LRANGE,
				respCallBack, key, String.valueOf(start), String.valueOf(end));
	}

	public void rPopLPush(ResponseCallback<String> respCallBack,
			String sourceKey, String destKey) {
		sendCommandWithStringResponseAdapter(RedisCommand.RPOPLPUSH,
				respCallBack, sourceKey, destKey);
	}

	public void bRPopLPush(ResponseCallback<String> respCallBack,
			String sourceKey, String destKey, long timeout) {
		sendCommandWithStringResponseAdapter(RedisCommand.BRPOPLPUSH,
				respCallBack, sourceKey, destKey, String.valueOf(timeout));
	}

	public void lIndex(ResponseCallback<String> respCallBack, String key,
			long index) {
		sendCommandWithStringResponseAdapter(RedisCommand.LINDEX, respCallBack,
				key, String.valueOf(index));
	}

	public void lInsert(ResponseCallback<Long> respCallBack, String key,
			String value, String pivot, boolean before) {
		sendCommandWithLongResponseAdapter(
				RedisCommand.LINSERT,
				respCallBack,
				key,
				before ? ListPosition.BEFORE.name() : ListPosition.AFTER.name(),
				pivot, value);
	}

	public void lLen(ResponseCallback<Long> respCallBack, String key) {
		sendCommandWithLongResponseAdapter(RedisCommand.LLEN, respCallBack, key);
	}

	public void lREM(ResponseCallback<Long> respCallBack, String key,
			String value, long count) {
		sendCommandWithLongResponseAdapter(RedisCommand.LREM, respCallBack,
				key, String.valueOf(count), value);
	}

	public void lSet(ResponseCallback<String> respCallBack, String key,
			long index, String value) {
		sendCommandWithStringResponseAdapter(RedisCommand.LSET, respCallBack,
				key, String.valueOf(index), value);
	}

	public void lTrim(ResponseCallback<String> respCallBack, String key,
			long start, long end) {
		sendCommandWithStringResponseAdapter(RedisCommand.LTRIM, respCallBack,
				key, String.valueOf(start), String.valueOf(end));
	}

	public void sAdd(ResponseCallback<Long> respCallBack, String key,
			String member, String... moreMember) {
		String[] args = CmdArgumentTool.combineArgs(
				new String[] { key, member }, moreMember);
		sendCommandWithLongResponseAdapter(RedisCommand.SADD, respCallBack,
				args);
	}

	public void sCard(ResponseCallback<Long> respCallBack, String key) {
		sendCommandWithLongResponseAdapter(RedisCommand.SCARD, respCallBack,
				key);
	}

	public void sPop(ResponseCallback<String> respCallBack, String key) {
		sendCommandWithStringResponseAdapter(RedisCommand.SPOP, respCallBack,
				key);
	}

	public void sRandMember(ResponseCallback<String[]> respCallBack, String key) {
		sendCommandWithStringArrayResponseAdapter(RedisCommand.SRANDMEMBER,
				respCallBack, key);
	}

	public void sRandMember(ResponseCallback<String[]> respCallBack,
			String key, int count) {
		String[] args = new String[] { key, String.valueOf(count) };
		sendCommandWithStringArrayResponseAdapter(RedisCommand.SRANDMEMBER,
				respCallBack, args);
	}

	public void sMembers(ResponseCallback<String[]> respCallBack, String key) {
		sendCommandWithStringArrayResponseAdapter(RedisCommand.SMEMBERS,
				respCallBack, key);
	}

	public void sDiff(ResponseCallback<String[]> respCallBack, String key,
			String... moreKeys) {
		String[] args = CmdArgumentTool.combineArgs(key, moreKeys);
		sendCommandWithStringArrayResponseAdapter(RedisCommand.SDIFF,
				respCallBack, args);
	}

	public void sDiffStore(ResponseCallback<Long> respCallBack, String destKey,
			String key, String... moreKeys) {
		String[] args = CmdArgumentTool.combineArgs(
				new String[] { destKey, key }, moreKeys);
		sendCommandWithLongResponseAdapter(RedisCommand.SDIFFSTORE,
				respCallBack, args);
	}

	public void sInter(ResponseCallback<String[]> respCallBack, String key,
			String... moreKeys) {
		String[] args = CmdArgumentTool.combineArgs(key, moreKeys);
		sendCommandWithStringArrayResponseAdapter(RedisCommand.SINTER,
				respCallBack, args);
	}

	public void sInterStore(ResponseCallback<Long> respCallBack,
			String destKey, String key, String... moreKeys) {
		String[] args = CmdArgumentTool.combineArgs(
				new String[] { destKey, key }, moreKeys);
		sendCommandWithLongResponseAdapter(RedisCommand.SINTERSTORE,
				respCallBack, args);
	}

	public void sisMember(ResponseCallback<Boolean> respCallBack, String key,
			String member) {
		sendCommandWithBoolResponseAdapter(RedisCommand.SISMEMBER,
				respCallBack, key, member);
	}

	public void sRem(ResponseCallback<Long> respCallBack, String key,
			String member, String... moreMembers) {
		String[] args = CmdArgumentTool.combineArgs(
				new String[] { key, member }, moreMembers);
		sendCommandWithLongResponseAdapter(RedisCommand.SREM, respCallBack,
				args);
	}

	public void sMove(ResponseCallback<Boolean> respCallBack, String source,
			String destination, String member) {
		sendCommandWithBoolResponseAdapter(RedisCommand.SMOVE, respCallBack,
				source, destination, member);
	}

	public void sUnion(ResponseCallback<String[]> respCallBack, String key,
			String... moreKeys) {
		String[] args = CmdArgumentTool.combineArgs(key, moreKeys);
		sendCommandWithStringArrayResponseAdapter(RedisCommand.SUNION,
				respCallBack, args);
	}

	public void sUnionStore(ResponseCallback<Long> respCallBack,
			String destKey, String key, String... moreKeys) {
		String[] args = CmdArgumentTool.combineArgs(
				new String[] { destKey, key }, moreKeys);
		sendCommandWithLongResponseAdapter(RedisCommand.SUNIONSTORE,
				respCallBack, args);
	}

	public void sScan(ResponseCallback<ScanResult<String>> respCallBack,
			String key, String cursor) {
		sScan(respCallBack, key, cursor, null);
	}

	public void sScan(ResponseCallback<ScanResult<String>> respCallBack,
			String key, String cursor, ScanParams params) {
		List<String> args = new ArrayList<>();
		args.add(key);
		args.add(cursor);
		if (params != null) {
			args.addAll(params.getParams());
		}
		sendCommandWithStringScanResultResponseAdapter(RedisCommand.SSCAN,
				respCallBack, args.toArray(new String[0]));
	}

	public void zAdd(ResponseCallback<Long> respCallBack, String key,
			double score, String member) {
		ScoreMemberPair pair = new ScoreMemberPair(score, member);
		zAdd(respCallBack, key, pair);
	}

	public void zAdd(ResponseCallback<Long> respCallBack, String key,
			ScoreMemberPair scoreMember, ScoreMemberPair... moreScoreMembers) {
		// if (moreScoreMembers == null) {
		// throw new IllegalArgumentException(
		// "Must pass a score member pair at least.");
		// }
		String[] args = CmdArgumentTool.combineArgs(scoreMember,
				moreScoreMembers);
		args = CmdArgumentTool.combineArgs(key, args);
		sendCommandWithLongResponseAdapter(RedisCommand.ZADD, respCallBack,
				args);
	}

	public void zCard(ResponseCallback<Long> respCallBack, String key) {
		sendCommandWithLongResponseAdapter(RedisCommand.ZCARD, respCallBack,
				key);
	}

	public void zCount(ResponseCallback<Long> respCallBack, String key,
			double min, double max) {
		sendCommandWithLongResponseAdapter(RedisCommand.ZCOUNT, respCallBack,
				key, String.valueOf(min), String.valueOf(max));

	}

	public void zIncrBy(ResponseCallback<Double> respCallBack, String key,
			String member, double increment) {
		sendCommandWithDoubleResponseAdapter(RedisCommand.ZINCRBY,
				respCallBack, key, String.valueOf(increment), member);
	}

	public void zRange(ResponseCallback<String[]> respCallBack, String key,
			long start, long end) {
		sendCommandWithStringArrayResponseAdapter(RedisCommand.ZRANGE,
				respCallBack, key, String.valueOf(start), String.valueOf(end));
	}

	public void zRangeWithScores(
			ResponseCallback<ScoreMemberPair[]> respCallBack, String key,
			long start, long end) {
		sendCommandWithScoreMemberPairResponseAdapter(RedisCommand.ZRANGE,
				respCallBack, key, String.valueOf(start), String.valueOf(end),
				RedisKeyword.WITHSCORES.name());
	}

	public void zRangeByScore(ResponseCallback<String[]> respCallBack,
			String key, double min, double max) {
		sendCommandWithStringArrayResponseAdapter(RedisCommand.ZRANGEBYSCORE,
				respCallBack, key, String.valueOf(min), String.valueOf(max));
	}

	public void zRangeByScore(ResponseCallback<String[]> respCallBack,
			String key, double min, double max, int offset, int count) {
		sendCommandWithStringArrayResponseAdapter(RedisCommand.ZRANGEBYSCORE,
				respCallBack, key, String.valueOf(min), String.valueOf(max),
				RedisKeyword.LIMIT.name(), String.valueOf(offset),
				String.valueOf(count));
	}

	public void zRangeByScoreWithScores(
			ResponseCallback<ScoreMemberPair[]> respCallBack, String key,
			double min, double max) {
		sendCommandWithScoreMemberPairResponseAdapter(
				RedisCommand.ZRANGEBYSCORE, respCallBack, key,
				String.valueOf(min), String.valueOf(max),
				RedisKeyword.WITHSCORES.name());
	}

	public void zRangeByScoreWithScores(
			ResponseCallback<ScoreMemberPair[]> respCallBack, String key,
			double min, double max, int offset, int count) {
		sendCommandWithScoreMemberPairResponseAdapter(
				RedisCommand.ZRANGEBYSCORE, respCallBack, key,
				String.valueOf(min), String.valueOf(max),
				RedisKeyword.WITHSCORES.name(), RedisKeyword.LIMIT.name(),
				String.valueOf(offset), String.valueOf(count));
	}

	public void zRank(ResponseCallback<Long> respCallBack, String key,
			String member) {
		sendCommandWithLongResponseAdapter(RedisCommand.ZRANK, respCallBack,
				key, member);
	}

	public void zRem(ResponseCallback<Long> respCallBack, String key,
			String member, String... moreMembers) {
		String[] args = CmdArgumentTool.combineArgs(
				new String[] { key, member }, moreMembers);
		sendCommandWithLongResponseAdapter(RedisCommand.ZREM, respCallBack,
				args);
	}

	public void zRemRangeByRank(ResponseCallback<Long> respCallBack,
			String key, long start, long end) {
		sendCommandWithLongResponseAdapter(RedisCommand.ZREMRANGEBYRANK,
				respCallBack, key, String.valueOf(start), String.valueOf(end));
	}

	public void zRemRangeByScore(ResponseCallback<Long> respCallBack,
			String key, double min, double max) {
		sendCommandWithLongResponseAdapter(RedisCommand.ZREMRANGEBYSCORE,
				respCallBack, key, String.valueOf(min), String.valueOf(max));
	}

	public void zRevRange(ResponseCallback<String[]> respCallBack, String key,
			long start, long end) {
		sendCommandWithStringArrayResponseAdapter(RedisCommand.ZREVRANGE,
				respCallBack, key, String.valueOf(start), String.valueOf(end));
	}

	public void zRevRangeWithScores(
			ResponseCallback<ScoreMemberPair[]> respCallBack, String key,
			long start, long end) {
		sendCommandWithScoreMemberPairResponseAdapter(RedisCommand.ZREVRANGE,
				respCallBack, key, String.valueOf(start), String.valueOf(end),
				RedisKeyword.WITHSCORES.name());
	}

	public void zRevRangeByScore(ResponseCallback<String[]> respCallBack,
			String key, double max, double min) {
		sendCommandWithStringArrayResponseAdapter(
				RedisCommand.ZREVRANGEBYSCORE, respCallBack, key,
				String.valueOf(max), String.valueOf(min));
	}

	public void zRevRangeByScore(ResponseCallback<String[]> respCallBack,
			String key, double max, double min, int offset, int count) {
		sendCommandWithStringArrayResponseAdapter(
				RedisCommand.ZREVRANGEBYSCORE, respCallBack, key,
				String.valueOf(max), String.valueOf(min),
				RedisKeyword.LIMIT.name(), String.valueOf(offset),
				String.valueOf(count));
	}

	public void zRevRangeByScoreWithScores(
			ResponseCallback<ScoreMemberPair[]> respCallBack, String key,
			double max, double min) {
		sendCommandWithScoreMemberPairResponseAdapter(
				RedisCommand.ZREVRANGEBYSCORE, respCallBack, key,
				String.valueOf(max), String.valueOf(min),
				RedisKeyword.WITHSCORES.name());
	}

	public void zRevRangeByScoreWithScores(
			ResponseCallback<ScoreMemberPair[]> respCallBack, String key,
			double max, double min, int offset, int count) {
		sendCommandWithScoreMemberPairResponseAdapter(
				RedisCommand.ZREVRANGEBYSCORE, respCallBack, key,
				String.valueOf(max), String.valueOf(min),
				RedisKeyword.WITHSCORES.name(), RedisKeyword.LIMIT.name(),
				String.valueOf(offset), String.valueOf(count));
	}

	public void zRevRank(ResponseCallback<Long> respCallBack, String key,
			String member) {
		sendCommandWithLongResponseAdapter(RedisCommand.ZREVRANK, respCallBack,
				key, member);
	}

	public void zScore(ResponseCallback<Double> respCallBack, String key,
			String member) {
		sendCommandWithDoubleResponseAdapter(RedisCommand.ZSCORE, respCallBack,
				key, member);
	}

	public void zUnionStore(ResponseCallback<Long> respCallBack,
			String destKey, String key, String... moreKeys) {
		long numberKeys = moreKeys == null ? 1 : moreKeys.length + 1;
		String[] args = CmdArgumentTool.combineArgs(new String[] { destKey,
				String.valueOf(numberKeys), key }, moreKeys);
		sendCommandWithLongResponseAdapter(RedisCommand.ZUNIONSTORE,
				respCallBack, args);
	}

	public void zUnionStore(ResponseCallback<Long> respCallBack,
			String destKey, SortedSetParams params, String key,
			String... moreKeys) {
		long numberKeys = moreKeys == null ? 1 : moreKeys.length + 1;
		String[] args = CmdArgumentTool.combineArgs(new String[] { destKey,
				String.valueOf(numberKeys), key }, moreKeys);
		args = CmdArgumentTool.combineArgs(args, params.getParams());
		sendCommandWithLongResponseAdapter(RedisCommand.ZUNIONSTORE,
				respCallBack, args);
	}

	public void zInterStore(ResponseCallback<Long> respCallBack,
			String destKey, String key, String... moreKeys) {
		long numberKeys = moreKeys == null ? 1 : moreKeys.length + 1;
		String[] args = CmdArgumentTool.combineArgs(new String[] { destKey,
				String.valueOf(numberKeys), key }, moreKeys);
		sendCommandWithLongResponseAdapter(RedisCommand.ZINTERSTORE,
				respCallBack, args);
	}

	public void zInterStore(ResponseCallback<Long> respCallBack,
			String destKey, SortedSetParams params, String key,
			String... moreKeys) {
		long numberKeys = moreKeys == null ? 1 : moreKeys.length + 1;
		String[] args = CmdArgumentTool.combineArgs(new String[] { destKey,
				String.valueOf(numberKeys), key }, moreKeys);
		args = CmdArgumentTool.combineArgs(args, params.getParams());
		sendCommandWithLongResponseAdapter(RedisCommand.ZINTERSTORE,
				respCallBack, args);
	}

	public void zScan(
			ResponseCallback<ScanResult<ScoreMemberPair>> respCallBack,
			String key, String cursor) {
		zScan(respCallBack, key, cursor, null);
	}

	public void zScan(
			ResponseCallback<ScanResult<ScoreMemberPair>> respCallBack,
			String key, String cursor, ScanParams params) {
		List<String> args = new ArrayList<>();
		args.add(key);
		args.add(cursor);
		if (params != null) {
			args.addAll(params.getParams());
		}
		sendCommandWithScoreMemberScanResultResponseAdapter(RedisCommand.ZSCAN,
				respCallBack, args.toArray(new String[0]));
	}

	public void select(ResponseCallback<String> respCallBack, long index) {
		sendCommandWithStringResponseAdapter(RedisCommand.SELECT, respCallBack,
				String.valueOf(index));
	}

	public void auth(ResponseCallback<String> respCallBack, String password) {
		sendCommandWithStringResponseAdapter(RedisCommand.AUTH, respCallBack,
				password);
	}

	public void echo(ResponseCallback<String> respCallBack, String message) {
		sendCommandWithStringResponseAdapter(RedisCommand.ECHO, respCallBack,
				message);
	}

	public void ping(ResponseCallback<String> respCallBack) {
		sendCommandWithStringResponseAdapter(RedisCommand.PING, respCallBack);
	}

	public void quit(ResponseCallback<String> respCallBack) {
		sendCommandWithStringResponseAdapter(RedisCommand.QUIT, respCallBack);
	}

	public void flushDB(ResponseCallback<String> respCallBack) {
		sendCommandWithStringResponseAdapter(RedisCommand.FLUSHDB, respCallBack);
	}

	public void flushAll(ResponseCallback<String> respCallBack) {
		sendCommandWithStringResponseAdapter(RedisCommand.FLUSHALL,
				respCallBack);
	}

	private void sendCommandWithStringResponseAdapter0(RedisCommand command,
			ResponseCallback<String> respCallBack, byte[]... args) {
		ResponeAdapter<Object, String> respAdapter = new StringResponseAdapter(
				respCallBack);
		sendCommand(command, respAdapter, args);

	}

	private void sendCommandWitByteResponseAdapter(RedisCommand command,
			ResponseCallback<byte[]> respCallBack, String... args) {
		ResponeAdapter<Object, byte[]> respAdapter = new ByteResponseAdapter(
				respCallBack);
		sendCommand(command, respAdapter, args);
	}

	private void sendCommandWithStringResponseAdapter(RedisCommand command,
			ResponseCallback<String> respCallBack, String... args) {
		ResponeAdapter<Object, String> respAdapter = new StringResponseAdapter(
				respCallBack);
		sendCommand(command, respAdapter, args);
	}

	private void sendCommandWithLongResponseAdapter(RedisCommand command,
			ResponseCallback<Long> respCallBack, String... args) {

		ResponeAdapter<Object, Long> respAdapter = new LongResponseAdapter(
				respCallBack);
		sendCommand(command, respAdapter, args);

	}

	private void sendCommandWithDoubleResponseAdapter(RedisCommand command,
			ResponseCallback<Double> respCallBack, String... args) {
		ResponeAdapter<Object, Double> respAdapter = new DoubleResponseAdapter(
				respCallBack);
		sendCommand(command, respAdapter, args);
	}

	private void sendCommandWithBoolResponseAdapter(RedisCommand command,
			ResponseCallback<Boolean> respCallBack, String... args) {
		ResponeAdapter<Object, Boolean> respAdapter = new BooleanResponseAdapter(
				respCallBack);
		sendCommand(command, respAdapter, args);
	}

	private void sendCommandWithStringArrayResponseAdapter(
			RedisCommand command, ResponseCallback<String[]> respCallBack,
			String... args) {
		ResponeAdapter<Object, String[]> respAdapter = new StringArrayResponseAdapter(
				respCallBack);
		sendCommand(command, respAdapter, args);
	}

	private void sendCommandWithStringScanResultResponseAdapter(
			RedisCommand command,
			ResponseCallback<ScanResult<String>> respCallBack, String... args) {
		ResponeAdapter<List<Object>, ScanResult<String>> respAdapter = new StringScanResultResponseAdapter(
				respCallBack);
		sendCommand(command, respAdapter, args);
	}

	private void sendCommandWithHashScanResultResponseAdapter(
			RedisCommand command,
			ResponseCallback<ScanResult<Entry<String, String>>> respCallBack,
			String... args) {
		ResponeAdapter<List<Object>, ScanResult<Entry<String, String>>> respAdapter = new HashScanResultResponseAdapter(
				respCallBack);
		sendCommand(command, respAdapter, args);
	}

	private void sendCommandWithHashMapResponseAdapter(RedisCommand command,
			ResponseCallback<Map<String, String>> respCallBack, String... args) {
		ResponeAdapter<List<Object>, Map<String, String>> respAdapter = new HashMapResponseAdapter(
				respCallBack);
		sendCommand(command, respAdapter, args);
	}

	private void sendCommandWithScoreMemberScanResultResponseAdapter(
			RedisCommand command,
			ResponseCallback<ScanResult<ScoreMemberPair>> respCallBack,
			String... args) {
		ResponeAdapter<List<Object>, ScanResult<ScoreMemberPair>> respAdapter = new ScoreMemberScanResultResponseAdapter(
				respCallBack);
		sendCommand(command, respAdapter, args);
	}

	private void sendCommandWithScoreMemberPairResponseAdapter(
			RedisCommand command,
			ResponseCallback<ScoreMemberPair[]> respCallBack, String... args) {
		ResponeAdapter<List<Object>, ScoreMemberPair[]> respAdapter = new ScoreMemberPairResponseAdapter(
				respCallBack);
		sendCommand(command, respAdapter, args);
	}

	private <S, T> void sendCommand(RedisCommand command,
			ResponeAdapter<S, T> responseAdapter, byte[]... args) {
		checkStatus();
		final BinaryCommand message = new BinaryCommand(command.getText(), args);
		try {
			connectionPool.sendCommand(message, responseAdapter);
		} catch (Throwable e) {
			LOGGER.log(Level.WARNING, e.getMessage(), e);
			if (responseAdapter != null) {
				responseAdapter.failed(e);
			}
		}
	}

	private <S, T> void sendCommand(RedisCommand command,
			ResponeAdapter<S, T> responseAdapter, String... args) {
		checkStatus();
		final BinaryCommand message = new BinaryCommand(command.getText(), args);
		try {
			connectionPool.sendCommand(message, responseAdapter);
		} catch (Throwable e) {
			LOGGER.log(Level.WARNING, e.getMessage(), e);
			if (responseAdapter != null) {
				responseAdapter.failed(e);
			}
		}
	}

	private void checkStatus() {
		if (!isInit) {
			throw new IllegalStateException(
					"The client has not been initialized correctly, you must call initialize() before using this client");
		}
	}

	@Override
	public NedisClient setMaxConnectionIdleTimeInMills(int mills) {
		this.maxConnectionIdleTimeInMills = mills;
		return this;
	}

	public int getIdleConnections() {
		checkStatus();
		return connectionPool.idleSize();
	}

	@Override
	public NedisClient setMinIdleConnections(int minIdle) {
		this.minIdleConnection = minIdle;
		return this;
	}

}
