package org.cyy.fw.nedis.util;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Sharding by consistent hashing, assisted by TreeMap and MD5 hash algorithm
 * 
 * @author yunyun
 * 
 * @param <T>
 */
public class NodeSharder<T extends NodeInfo> {

	private TreeMap<Long, T> nodesCache;

	public NodeSharder(List<T> shards) {
		super();
		init(shards);
	}

	public void init(List<T> nodes) {
		nodesCache = new TreeMap<>();
		if (nodes == null) {
			return;
		}
		int nodeSize = nodes.size();
		// int totalWeigth = 0;
		// for (int i = 0; i < nodeSize; i++) {
		// totalWeigth += nodes.get(i).getWeight();
		// }
		for (int i = 0; i < nodeSize; i++) {
			T node = nodes.get(i);
			int weight = node.getWeight();
			// double factor = Math.floor(((double) (160 * nodeSize * weight))
			// / (double) totalWeigth);
			int n = 160 * weight;
			for (long j = 0; j < n; j++) {
				if (node.getName() == null) {
					nodesCache.put(MD5Hasher.hash("SHARD-" + i + "-NODE-" + j),
							node);

				} else {
					nodesCache.put(
							MD5Hasher.hash(node.getName() + "*" + weight + j),
							node);
				}
			}
		}
	}

	public T getShardNodeInfo(String key) {
		return getShardNodeInfo(TextEncoder.encode(key));
	}

	public T getShardNodeInfo(byte[] key) {
		SortedMap<Long, T> tail = nodesCache.tailMap(MD5Hasher.hash(key));
		if (tail.isEmpty()) {
			return nodesCache.get(nodesCache.firstKey());
		}
		return tail.get(tail.firstKey());
	}
}
