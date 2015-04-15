package org.cyy.fw.nedis.test.cmd;

import static org.junit.Assert.assertTrue;

import org.cyy.fw.nedis.NedisClient;
import org.cyy.fw.nedis.NedisClientBuilder;
import org.cyy.fw.nedis.RedisProtocol;
import org.cyy.fw.nedis.test.BaseTest;
import org.junit.After;
import org.junit.Before;

public class BaseCmdTest extends BaseTest {

	protected NedisClient client;

	@Before
	public void init() {
		String host = "192.168.1.107";
		int port = 6379;
		// client = new NedisClient().setHost(host).setPort(port);
		client = new NedisClientBuilder().setServerHost(host).setPort(port)
				.setConnectTimeoutMills(connectTimeoutMills)
				.setConnectionPoolSize(5).build();
	}

	@After
	public void destroy() {
		client.shutdown();
	}

	protected void assertResultNull(String result) {
		assertTrue(RedisProtocol.isNull(result));
	}
}
