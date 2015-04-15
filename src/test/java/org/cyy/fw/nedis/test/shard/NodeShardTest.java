package org.cyy.fw.nedis.test.shard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.cyy.fw.nedis.ServerNode;
import org.cyy.fw.nedis.util.NodeSharder;
import org.junit.Before;
import org.junit.Test;

public class NodeShardTest {

	// private static final Logger LOGGER = Logger.getLogger(NedisClient.class
	// .getName());
	private NodeSharder<ServerNode> sharder;

	@Before
	public void init() {
		List<ServerNode> nodes = new ArrayList<>();
		ServerNode node1 = new ServerNode();
		node1.setHost("192.168.1.101");
		node1.setPort(6379);
		ServerNode node2 = new ServerNode();
		node2.setHost("192.168.1.102");
		node2.setPort(6379);
		ServerNode node3 = new ServerNode();
		node3.setHost("192.168.1.103");
		node3.setPort(6379);
		ServerNode node4 = new ServerNode();
		node4.setHost("192.168.1.104");
		node4.setPort(6379);
		ServerNode node5 = new ServerNode();
		node5.setHost("192.168.1.105");
		node5.setPort(6379);
		ServerNode node6 = new ServerNode();
		node6.setHost("192.168.1.106");
		node6.setPort(6379);
		ServerNode node7 = new ServerNode();
		node7.setHost("192.168.1.107");
		node7.setPort(6379);
		ServerNode node8 = new ServerNode();
		node8.setHost("192.168.1.108");
		node8.setPort(6379);
		ServerNode node9 = new ServerNode();
		node9.setHost("192.168.1.109");
		node9.setPort(6379);
		ServerNode node10 = new ServerNode();
		node10.setHost("192.168.1.100");
		node10.setPort(6379);
		nodes.add(node1);
		nodes.add(node2);
		nodes.add(node3);
		nodes.add(node4);
		nodes.add(node5);
		nodes.add(node6);
		nodes.add(node7);
		nodes.add(node8);
		nodes.add(node9);
		nodes.add(node10);
		sharder = new NodeSharder<>(nodes);
	}

	@Test
	public void testShard() {
		int testNumber = 100000;
		Map<String, Integer> result = new HashMap<>();
		for (int i = 0; i < testNumber; i++) {
			String key = "key-" + i;
			ServerNode node = sharder.getShardNodeInfo(key);
			String host = node.getHost();
			Integer number = result.get(host);
			if (number == null) {
				number = 0;
			}
			result.put(host, ++number);
		}
		for (Entry<String, Integer> entry : result.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
	}

}
