package org.cyy.fw.nedis;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cyy.fw.nedis.util.NodeSharder;

/**
 * Sharding keys, distribute keys evenly by consistent hashing.
 * 
 * @author yunyun
 * 
 */
public class ShardedNedis implements ClientConfig<ShardedNedis> {
	private List<ServerNode> nodes;
	private int connectTimeoutMills;
	private Map<String, NedisClient> serverClientMapping;
	private NodeSharder<ServerNode> nodeSharder;
	private int eventLoopGroupSize;
	private int connectionPoolSize;
	private boolean isTcpNoDelay = true;
	private ByteBufAllocator allocator;
	private RecvByteBufAllocator recvAllocator;
	private MessageSizeEstimator estimator;
	private int maxConnectionIdleTimeInMills;
	private int minIdleConnection;

	public ShardedNedis(List<ServerNode> nodes) {
		nodeSharder = new NodeSharder<>(nodes);
		serverClientMapping = new HashMap<>();
		this.nodes = nodes;
	}

	public ShardedNedis setConnectTimeoutMills(int connectTimeoutMills) {
		this.connectTimeoutMills = connectTimeoutMills;
		return this;
	}

	@Override
	public ShardedNedis setEventLoopGroupSize(int eventLoopGroupSize) {
		this.eventLoopGroupSize = eventLoopGroupSize;
		return this;
	}

	/**
	 * Shutdown all client instances
	 */
	public void shutdown() {
		Collection<NedisClient> clients = serverClientMapping.values();
		for (NedisClient client : clients) {
			if (client == null) {
				continue;
			}
			client.shutdown();
		}
		serverClientMapping.clear();
	}

	private <T> ResponseCallback<T> wrapShardedCallBack(
			final ResponseCallback<ShardedResponse<T>> respCallBack,
			final ServerNode server) {
		return new ResponseCallback<T>() {

			@Override
			public void failed(Throwable cause) {
				if (respCallBack == null) {
					return;
				}
				respCallBack.failed(new ShardedException(cause, server));
			}

			@Override
			public void done(T result) {
				if (respCallBack == null) {
					return;
				}
				respCallBack.done(new ShardedResponse<T>(result, server));
			}
		};
	}

	public void flushAll(ResponseCallback<ShardedResponse<String>> respCallBack) {
		if (this.nodes == null) {
			return;
		}
		for (ServerNode node : nodes) {
			NedisClient client = getClient(node);
			client.flushAll(wrapShardedCallBack(respCallBack, node));
		}
	}

	public void flushDB(ResponseCallback<ShardedResponse<String>> respCallBack) {
		if (this.nodes == null) {
			return;
		}
		for (ServerNode node : nodes) {
			NedisClient client = getClient(node);
			// client.flushDB(respCallBack);
			client.flushDB(wrapShardedCallBack(respCallBack, node));
		}
	}

	public void expire(ResponseCallback<ShardedResponse<Boolean>> respCallBack,
			String key, long seconds) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).expire(
				wrapShardedCallBack(respCallBack, serverNode), key, seconds);
	}

	public void expireAt(
			ResponseCallback<ShardedResponse<Boolean>> respCallBack,
			String key, long ts) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).expireAt(
				wrapShardedCallBack(respCallBack, serverNode), key, ts);
	}

	public void move(ResponseCallback<ShardedResponse<Boolean>> respCallBack,
			String key, int destDb) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).move(
				wrapShardedCallBack(respCallBack, serverNode), key, destDb);
	}

	public void objectRefcount(
			ResponseCallback<ShardedResponse<Long>> respCallBack, String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).objectRefcount(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void objectIdletime(
			ResponseCallback<ShardedResponse<Long>> respCallBack, String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).objectIdletime(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void objectEncoding(
			ResponseCallback<ShardedResponse<String>> respCallBack, String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).objectEncoding(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void persist(
			ResponseCallback<ShardedResponse<Boolean>> respCallBack, String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).persist(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void pExpire(
			ResponseCallback<ShardedResponse<Boolean>> respCallBack,
			String key, long millSeconds) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode)
				.pExpire(wrapShardedCallBack(respCallBack, serverNode), key,
						millSeconds);
	}

	public void pExpireAt(
			ResponseCallback<ShardedResponse<Boolean>> respCallBack,
			String key, long millTs) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).pExpireAt(
				wrapShardedCallBack(respCallBack, serverNode), key, millTs);
	}

	public void pTTL(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).pTTL(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void sort(ResponseCallback<ShardedResponse<String[]>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).sort(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void sort(ResponseCallback<ShardedResponse<String[]>> respCallBack,
			String key, SortingParams params) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).sort(
				wrapShardedCallBack(respCallBack, serverNode), key, params);
	}

	public void ttl(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).ttl(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void type(ResponseCallback<ShardedResponse<String>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).type(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void set(ResponseCallback<ShardedResponse<String>> respCallBack,
			String key, String value) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).set(
				wrapShardedCallBack(respCallBack, serverNode), key, value);
	}

	public void get(ResponseCallback<ShardedResponse<String>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).get(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void del(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).del(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void exists(ResponseCallback<ShardedResponse<Boolean>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).exists(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void append(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String value) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).append(
				wrapShardedCallBack(respCallBack, serverNode), key, value);
	}

	public void setBit(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, long offset, int value) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).setBit(
				wrapShardedCallBack(respCallBack, serverNode), key, offset,
				value);
	}

	public void getBit(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, long offset) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).getBit(
				wrapShardedCallBack(respCallBack, serverNode), key, offset);
	}

	public void bitCount(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).bitCount(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void bitCount(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, long start, long end) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).bitCount(
				wrapShardedCallBack(respCallBack, serverNode), key, start, end);
	}

	public void incr(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).incr(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void incrBy(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, long increment) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).incrBy(
				wrapShardedCallBack(respCallBack, serverNode), key, increment);
	}

	public void incrByFloat(
			ResponseCallback<ShardedResponse<Double>> respCallBack, String key,
			double increment) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).incrByFloat(
				wrapShardedCallBack(respCallBack, serverNode), key, increment);
	}

	public void decr(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).decr(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void decrBy(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, long increment) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).decrBy(
				wrapShardedCallBack(respCallBack, serverNode), key, increment);
	}

	public void getRange(
			ResponseCallback<ShardedResponse<String>> respCallBack, String key,
			long start, long end) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).getRange(
				wrapShardedCallBack(respCallBack, serverNode), key, start, end);
	}

	public void getSet(ResponseCallback<ShardedResponse<String>> respCallBack,
			String key, String value) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).getSet(
				wrapShardedCallBack(respCallBack, serverNode), key, value);
	}

	public void setNX(ResponseCallback<ShardedResponse<Boolean>> respCallBack,
			String key, String value) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).setNX(
				wrapShardedCallBack(respCallBack, serverNode), key, value);
	}

	public void setEX(ResponseCallback<ShardedResponse<String>> respCallBack,
			String key, long seconds, String value) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).setEX(
				wrapShardedCallBack(respCallBack, serverNode), key, seconds,
				value);
	}

	public void pSetEX(ResponseCallback<ShardedResponse<String>> respCallBack,
			String key, long millSeconds, String value) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).pSetEX(
				wrapShardedCallBack(respCallBack, serverNode), key,
				millSeconds, value);
	}

	public void setRange(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, long offset, String value) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).setRange(
				wrapShardedCallBack(respCallBack, serverNode), key, offset,
				value);
	}

	public void strLen(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).strLen(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void hSet(ResponseCallback<ShardedResponse<Boolean>> respCallBack,
			String key, String field, String value) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).hSet(
				wrapShardedCallBack(respCallBack, serverNode), key, field,
				value);
	}

	public void hSetNX(ResponseCallback<ShardedResponse<Boolean>> respCallBack,
			String key, String field, String value) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).hSetNX(
				wrapShardedCallBack(respCallBack, serverNode), key, field,
				value);
	}

	public void hGet(ResponseCallback<ShardedResponse<String>> respCallBack,
			String key, String field) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).hGet(
				wrapShardedCallBack(respCallBack, serverNode), key, field);
	}

	public void hGetAll(
			ResponseCallback<ShardedResponse<Map<String, String>>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).hGetAll(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void hDel(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String field) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).hDel(
				wrapShardedCallBack(respCallBack, serverNode), key, field);
	}

	public void hExist(ResponseCallback<ShardedResponse<Boolean>> respCallBack,
			String key, String field) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).hExist(
				wrapShardedCallBack(respCallBack, serverNode), key, field);
	}

	public void hIncrBy(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String field, long increment) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).hIncrBy(
				wrapShardedCallBack(respCallBack, serverNode), key, field,
				increment);
	}

	public void hIncrByFloat(
			ResponseCallback<ShardedResponse<Double>> respCallBack, String key,
			String field, double increment) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).hIncrByFloat(
				wrapShardedCallBack(respCallBack, serverNode), key, field,
				increment);
	}

	public void hKeys(ResponseCallback<ShardedResponse<String[]>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).hKeys(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void hVals(ResponseCallback<ShardedResponse<String[]>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).hVals(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void hLen(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).hLen(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void lPush(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String value, String... morvalue) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).lPush(
				wrapShardedCallBack(respCallBack, serverNode), key, value,
				morvalue);
	}

	public void lPushX(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String value) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).lPushX(
				wrapShardedCallBack(respCallBack, serverNode), key, value);
	}

	public void lPop(ResponseCallback<ShardedResponse<String>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).lPop(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void rPush(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String value, String... moreValues) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).rPush(
				wrapShardedCallBack(respCallBack, serverNode), key, value,
				moreValues);
	}

	public void rPushX(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String value, String... moreValues) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).rPushX(
				wrapShardedCallBack(respCallBack, serverNode), key, value,
				moreValues);
	}

	public void rPop(ResponseCallback<ShardedResponse<String>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).rPop(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void blPop(ResponseCallback<ShardedResponse<String[]>> respCallBack,
			long timeout, String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).blPop(
				wrapShardedCallBack(respCallBack, serverNode), timeout, key);
	}

	public void brPop(ResponseCallback<ShardedResponse<String[]>> respCallBack,
			long timeout, String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).brPop(
				wrapShardedCallBack(respCallBack, serverNode), timeout, key);
	}

	public void lRange(
			ResponseCallback<ShardedResponse<String[]>> respCallBack,
			String key, long start, long end) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).lRange(
				wrapShardedCallBack(respCallBack, serverNode), key, start, end);
	}

	public void lIndex(ResponseCallback<ShardedResponse<String>> respCallBack,
			String key, long index) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).lIndex(
				wrapShardedCallBack(respCallBack, serverNode), key, index);
	}

	public void lInsert(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String value, String pivot, boolean before) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).lInsert(
				wrapShardedCallBack(respCallBack, serverNode), key, value,
				pivot, before);
	}

	public void lLen(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).lLen(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void lREM(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String value, long count) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).lREM(
				wrapShardedCallBack(respCallBack, serverNode), key, value,
				count);
	}

	public void lSet(ResponseCallback<ShardedResponse<String>> respCallBack,
			String key, long index, String value) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).lSet(
				wrapShardedCallBack(respCallBack, serverNode), key, index,
				value);
	}

	public void lTrim(ResponseCallback<ShardedResponse<String>> respCallBack,
			String key, long start, long end) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).lTrim(
				wrapShardedCallBack(respCallBack, serverNode), key, start, end);
	}

	public void sAdd(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String member, String... moreMember) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).sAdd(
				wrapShardedCallBack(respCallBack, serverNode), key, member,
				moreMember);
	}

	public void sCard(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).sCard(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void sPop(ResponseCallback<ShardedResponse<String>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).sPop(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void sRandMember(
			ResponseCallback<ShardedResponse<String[]>> respCallBack, String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).sRandMember(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void sRandMember(
			ResponseCallback<ShardedResponse<String[]>> respCallBack,
			String key, int count) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).sRandMember(
				wrapShardedCallBack(respCallBack, serverNode), key, count);
	}

	public void sMembers(
			ResponseCallback<ShardedResponse<String[]>> respCallBack, String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).sMembers(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void sisMember(
			ResponseCallback<ShardedResponse<Boolean>> respCallBack,
			String key, String member) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).sisMember(
				wrapShardedCallBack(respCallBack, serverNode), key, member);
	}

	public void sRem(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String member, String... moreMembers) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).sRem(
				wrapShardedCallBack(respCallBack, serverNode), key, member,
				moreMembers);
	}

	public void zAdd(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, double score, String member) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zAdd(
				wrapShardedCallBack(respCallBack, serverNode), key, score,
				member);
	}

	public void zAdd(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, ScoreMemberPair scoreMember,
			ScoreMemberPair... moreScoreMembers) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zAdd(
				wrapShardedCallBack(respCallBack, serverNode), key,
				scoreMember, moreScoreMembers);
	}

	public void zCard(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zCard(
				wrapShardedCallBack(respCallBack, serverNode), key);
	}

	public void zCount(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, double min, double max) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zCount(
				wrapShardedCallBack(respCallBack, serverNode), key, min, max);
	}

	public void zIncrBy(ResponseCallback<ShardedResponse<Double>> respCallBack,
			String key, String member, double increment) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zIncrBy(
				wrapShardedCallBack(respCallBack, serverNode), key, member,
				increment);
	}

	public void zRange(
			ResponseCallback<ShardedResponse<String[]>> respCallBack,
			String key, long start, long end) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRange(
				wrapShardedCallBack(respCallBack, serverNode), key, start, end);
	}

	public void zRangeWithScores(
			ResponseCallback<ShardedResponse<ScoreMemberPair[]>> respCallBack,
			String key, long start, long end) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRangeWithScores(
				wrapShardedCallBack(respCallBack, serverNode), key, start, end);
	}

	public void zRangeByScore(
			ResponseCallback<ShardedResponse<String[]>> respCallBack,
			String key, double min, double max) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRangeByScore(
				wrapShardedCallBack(respCallBack, serverNode), key, min, max);
	}

	public void zRangeByScore(
			ResponseCallback<ShardedResponse<String[]>> respCallBack,
			String key, double min, double max, int offset, int count) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRangeByScore(
				wrapShardedCallBack(respCallBack, serverNode), key, min, max,
				offset, count);
	}

	public void zRangeByScoreWithScores(
			ResponseCallback<ShardedResponse<ScoreMemberPair[]>> respCallBack,
			String key, double min, double max) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRangeByScoreWithScores(
				wrapShardedCallBack(respCallBack, serverNode), key, min, max);
	}

	public void zRangeByScoreWithScores(
			ResponseCallback<ShardedResponse<ScoreMemberPair[]>> respCallBack,
			String key, double min, double max, int offset, int count) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRangeByScoreWithScores(
				wrapShardedCallBack(respCallBack, serverNode), key, min, max,
				offset, count);
	}

	public void zRank(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String member) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRank(
				wrapShardedCallBack(respCallBack, serverNode), key, member);
	}

	public void zRem(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String member, String... moreMembers) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRem(
				wrapShardedCallBack(respCallBack, serverNode), key, member,
				moreMembers);
	}

	public void zRemRangeByRank(
			ResponseCallback<ShardedResponse<Long>> respCallBack, String key,
			long start, long end) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRemRangeByRank(
				wrapShardedCallBack(respCallBack, serverNode), key, start, end);
	}

	public void zRemRangeByScore(
			ResponseCallback<ShardedResponse<Long>> respCallBack, String key,
			double min, double max) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRemRangeByScore(
				wrapShardedCallBack(respCallBack, serverNode), key, min, max);
	}

	public void zRevRange(
			ResponseCallback<ShardedResponse<String[]>> respCallBack,
			String key, long start, long end) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRevRange(
				wrapShardedCallBack(respCallBack, serverNode), key, start, end);
	}

	public void zRevRangeWithScores(
			ResponseCallback<ShardedResponse<ScoreMemberPair[]>> respCallBack,
			String key, long start, long end) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRevRangeWithScores(
				wrapShardedCallBack(respCallBack, serverNode), key, start, end);
	}

	public void zRevRangeByScore(
			ResponseCallback<ShardedResponse<String[]>> respCallBack,
			String key, double max, double min) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRevRangeByScore(
				wrapShardedCallBack(respCallBack, serverNode), key, max, min);
	}

	public void zRevRangeByScore(
			ResponseCallback<ShardedResponse<String[]>> respCallBack,
			String key, double max, double min, int offset, int count) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRevRangeByScore(
				wrapShardedCallBack(respCallBack, serverNode), key, max, min,
				offset, count);
	}

	public void zRevRangeByScoreWithScores(
			ResponseCallback<ShardedResponse<ScoreMemberPair[]>> respCallBack,
			String key, double max, double min) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRevRangeByScoreWithScores(
				wrapShardedCallBack(respCallBack, serverNode), key, max, min);
	}

	public void zRevRangeByScoreWithScores(
			ResponseCallback<ShardedResponse<ScoreMemberPair[]>> respCallBack,
			String key, double max, double min, int offset, int count) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRevRangeByScoreWithScores(
				wrapShardedCallBack(respCallBack, serverNode), key, max, min,
				offset, count);
	}

	public void zRevRank(ResponseCallback<ShardedResponse<Long>> respCallBack,
			String key, String member) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		getClient(serverNode).zRevRank(
				wrapShardedCallBack(respCallBack, serverNode), key, member);
	}

	public void zScore(ResponseCallback<ShardedResponse<Double>> respCallBack,
			String key, String member) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
		NedisClient nedisClient = getClient(serverNode);
		nedisClient.zScore(wrapShardedCallBack(respCallBack, serverNode), key,
				member);
	}

	public void echo(ResponseCallback<ShardedResponse<String>> respCallBack,
			String message) {
		ServerNode serverNode = nodeSharder.getShardNodeInfo(message);
		getClient(serverNode).echo(
				wrapShardedCallBack(respCallBack, serverNode), message);
	}

	// private NedisClient getClient(String key) {
	// ServerNode serverNode = nodeSharder.getShardNodeInfo(key);
	// return getClient(serverNode);
	// }

	private NedisClient getClient(ServerNode serverNode) {
		if (serverNode == null) {
			throw new NullPointerException("Must connect a server node.");
		}
		String serverStr = serverNode.toString();
		NedisClient client = serverClientMapping.get(serverStr);
		if (client != null) {
			return client;
		}
		RedisClientBuilder builder = new NedisClientBuilder()
				.setServerNode(serverNode)
				.setConnectTimeoutMills(connectTimeoutMills)
				.setEventLoopGroupSize(eventLoopGroupSize)
				.setTcpNoDelay(isTcpNoDelay).setByteBufAllocator(allocator)
				.setRecvByteBufAllocator(recvAllocator)
				.setMessageSizeEstimator(estimator)
				.setConnectionPoolSize(connectionPoolSize)
				.setMaxConnectionIdleTimeInMills(maxConnectionIdleTimeInMills)
				.setMinIdleConnections(minIdleConnection);
		client = builder.build();
		serverClientMapping.put(serverStr, client);
		return client;
	}

	@Override
	public ShardedNedis setTcpNoDelay(boolean flag) {
		this.isTcpNoDelay = flag;
		return this;
	}

	@Override
	public ShardedNedis setByteBufAllocator(ByteBufAllocator allocator) {
		this.allocator = allocator;
		return this;
	}

	@Override
	public ShardedNedis setRecvByteBufAllocator(
			RecvByteBufAllocator recvAllocator) {
		this.recvAllocator = recvAllocator;
		return this;
	}

	@Override
	public ShardedNedis setMessageSizeEstimator(MessageSizeEstimator estimator) {
		this.estimator = estimator;
		return this;
	}

	@Override
	public ShardedNedis setConnectionPoolSize(int poolSize) {
		this.connectionPoolSize = poolSize;
		return this;
	}

	@Override
	public ShardedNedis setMaxConnectionIdleTimeInMills(int mills) {
		this.maxConnectionIdleTimeInMills = mills;
		return this;
	}

	@Override
	public ShardedNedis setMinIdleConnections(int minIdle) {
		this.minIdleConnection = minIdle;
		return this;
	}
}
