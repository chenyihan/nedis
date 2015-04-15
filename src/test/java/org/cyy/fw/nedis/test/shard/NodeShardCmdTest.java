package org.cyy.fw.nedis.test.shard;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.cyy.fw.nedis.ResponseCallback;
import org.cyy.fw.nedis.ServerNode;
import org.cyy.fw.nedis.ShardedNedis;
import org.cyy.fw.nedis.ShardedResponse;
import org.cyy.fw.nedis.test.BaseTest;
import org.cyy.fw.nedis.util.NedisException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NodeShardCmdTest extends BaseTest {

	private ShardedNedis shardedNedis;

	@Before
	public void init() {
		ServerNode node1 = new ServerNode();
		node1.setHost("192.168.1.105");
		node1.setPort(6379);

		ServerNode node2 = new ServerNode();
		node2.setHost("192.168.1.107");
		node2.setPort(6379);
		List<ServerNode> nodeList = new ArrayList<>();
		nodeList.add(node1);
		nodeList.add(node2);
		shardedNedis = new ShardedNedis(nodeList).setConnectTimeoutMills(10000)
				.setConnectionPoolSize(5);
	}

	@After
	public void destroy() {
		shardedNedis.shutdown();
	}

	@Test
	public void testExpire() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				int repeats = 10;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(null, key, value);
					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.expire(
							new ResponseCallback<ShardedResponse<Boolean>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause.getCause());
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void done(ShardedResponse<Boolean> result) {
									System.out.println(result.getServer());
									assertEquals(true, result.getResult());
									if (isLast) {
										controller.countDown();
									}
								}
							}, key, 30);
				}

			}
		});
	}

	@Test
	public void testExpireAt() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(null, key, value);
					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.expireAt(
							new ResponseCallback<ShardedResponse<Boolean>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause.getCause());
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void done(ShardedResponse<Boolean> result) {
									System.out.println(result.getServer());
									assertEquals(true, result.getResult());
									if (isLast) {
										controller.countDown();
									}
								}
							}, key, 2355292000l);
				}

			}
		});
	}

	@Test
	public void testMove() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					final String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(null, key, value);
					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.move(
							new ResponseCallback<ShardedResponse<Boolean>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause.getCause());
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void done(ShardedResponse<Boolean> result) {
									System.out.println(result.getServer());
									System.out.println(key);
									assertEquals(true, result.getResult());
									if (isLast) {
										controller.countDown();
									}
								}
							}, key, 1);
				}

			}
		});
	}

	@Test
	public void testObjectRefcount() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(null, key, value);
					Thread.sleep(CMD_PAUSE_TIME);
					shardedNedis.objectRefcount(
							new ResponseCallback<ShardedResponse<Long>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause.getCause());
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void done(ShardedResponse<Long> result) {
									System.out.println(result.getServer());
									assertEquals(1, result.getResult()
											.longValue());
									if (isLast) {
										controller.countDown();
									}
								}
							}, key);
				}

			}
		});
	}

	@Test
	public void testObjectIdletime() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				int repeats = 10;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(null, key, value);
					Thread.sleep(CMD_PAUSE_TIME);
					shardedNedis.objectIdletime(
							new ResponseCallback<ShardedResponse<Long>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause);
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void done(ShardedResponse<Long> result) {
									System.out.println("ilde time:"
											+ result.getResult());
									if (isLast) {
										controller.countDown();
									}
								}
							}, key);
				}

			}
		});
	}

	@Test
	public void testObjectEncoding() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(null, key, value);
					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.objectEncoding(
							new ResponseCallback<ShardedResponse<String>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause.getCause());
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void done(ShardedResponse<String> result) {
									System.out.println("encoding:" + result);
									if (isLast) {
										controller.countDown();
									}
								}
							}, key);
				}

			}
		});
	}

	@Test
	public void testPersist() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(null, key, value);
					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.expire(null, key, 10);
					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.persist(
							new ResponseCallback<ShardedResponse<Boolean>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause);
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void done(ShardedResponse<Boolean> result) {
									System.out.println(result);
									assertEquals(true, result.getResult());
									if (isLast) {
										controller.countDown();
									}
								}
							}, key);
				}

			}
		});
	}

	@Test
	public void testPExpire() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(null, key, value);
					Thread.sleep(CMD_PAUSE_TIME);
					shardedNedis.pExpire(
							new ResponseCallback<ShardedResponse<Boolean>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause);
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void done(ShardedResponse<Boolean> result) {
									System.out.println(result);
									assertEquals(true, result.getResult());
									if (isLast) {
										controller.countDown();
									}
								}
							}, key, 30);
				}

			}
		});
	}

	@Test
	public void testPExpireAt() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(null, key, value);
					Thread.sleep(CMD_PAUSE_TIME);
					shardedNedis.pExpireAt(
							new ResponseCallback<ShardedResponse<Boolean>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause);
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void done(ShardedResponse<Boolean> result) {
									System.out.println(result);
									assertEquals(true, result.getResult());
									if (isLast) {
										controller.countDown();
									}
								}
							}, key, 2355292000l);
				}

			}
		});
	}

	@Test
	public void testTTL() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(null, key, value);
					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.expireAt(null, key, 2355292l);
					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.ttl(
							new ResponseCallback<ShardedResponse<Long>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause);
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void done(ShardedResponse<Long> result) {
									System.out.println("ttl:" + result);
									if (isLast) {
										controller.countDown();
									}
								}
							}, key);
				}

			}
		});
	}

	@Test
	public void testPTTL() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(null, key, value);
					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.pExpireAt(null, key, 2355292000l);
					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.pTTL(
							new ResponseCallback<ShardedResponse<Long>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause);
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void done(ShardedResponse<Long> result) {
									System.out.println("pttl:" + result);
									if (isLast) {
										controller.countDown();
									}
								}
							}, key);
				}

			}
		});
	}

	@Test
	public void testSort() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.lPush(null, key, "25", new String[] { "3",
							"5", "4", "55", "34", "15", "2" });
					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.sort(
							new ResponseCallback<ShardedResponse<String[]>>() {

								@Override
								public void done(
										ShardedResponse<String[]> result) {
									System.out.println(result.getServer());
									for (String r : result.getResult()) {
										System.out.println(r);
									}
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void failed(Throwable cause) {
									fail(cause);
									if (isLast) {
										controller.countDown();
									}
								}
							}, key);

				}

			}
		});
	}

	@Test
	public void testSetAndGet() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				int repeats = 10;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(
							new ResponseCallback<ShardedResponse<String>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause.getCause());
								}

								@Override
								public void done(ShardedResponse<String> result) {
									System.out.println(result.getServer());
									assertEquals("OK", result.getResult());
								}
							}, key, value);
					Thread.sleep(CMD_PAUSE_TIME);
					shardedNedis.get(
							new ResponseCallback<ShardedResponse<String>>() {

								@Override
								public void done(ShardedResponse<String> result) {
									System.out.println(result.getServer());
									System.out.println("result:"
											+ result.getResult());
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void failed(Throwable cause) {
									fail(cause.getCause());
									if (isLast) {
										controller.countDown();
									}
								}
							}, key);
				}

			}
		});
	}

	@Test
	public void testType() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(null, key, value);
					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.type(
							new ResponseCallback<ShardedResponse<String>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause);
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void done(ShardedResponse<String> result) {
									System.out.println("type" + result);
									if (isLast) {
										controller.countDown();
									}
								}
							}, key);
				}

			}
		});
	}

	@Test
	public void testSetAndDel() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(
							new ResponseCallback<ShardedResponse<String>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause.getCause());
								}

								@Override
								public void done(ShardedResponse<String> result) {
									System.out.println(result.getServer());
									assertEquals("OK", result.getResult());
								}
							}, key, value);
					Thread.sleep(CMD_PAUSE_TIME);
					shardedNedis.del(
							new ResponseCallback<ShardedResponse<Long>>() {

								@Override
								public void done(ShardedResponse<Long> result) {
									System.out.println("result:" + result);
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void failed(Throwable cause) {
									fail(cause);
									if (isLast) {
										controller.countDown();
									}
								}
							}, key);
				}

			}
		});
	}

	@Test
	public void testExist() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.set(null, key, value);
					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.exists(
							new ResponseCallback<ShardedResponse<Boolean>>() {

								@Override
								public void done(ShardedResponse<Boolean> result) {
									System.out.println(result);
									assertEquals(true, result.getResult());
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void failed(Throwable cause) {
									fail(cause);
									if (isLast) {
										controller.countDown();
									}
								}
							}, key);
				}

			}
		});
	}

	@Test
	public void testAppend() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					// String value = "value-" + i;
					final boolean isLast = i == repeats - 1;

					shardedNedis.append(
							new ResponseCallback<ShardedResponse<Long>>() {

								@Override
								public void done(ShardedResponse<Long> result) {
									System.out.println(result);
									assertEquals(Long.valueOf("111".length()),
											result.getResult());
								}

								@Override
								public void failed(Throwable cause) {
									fail(cause);
								}
							}, key, "111");

					Thread.sleep(CMD_PAUSE_TIME);

					shardedNedis.append(
							new ResponseCallback<ShardedResponse<Long>>() {

								@Override
								public void done(ShardedResponse<Long> result) {
									System.out.println(result);
									assertEquals(
											Long.valueOf("111222".length()),
											result.getResult());
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void failed(Throwable cause) {
									fail(cause);
									if (isLast) {
										controller.countDown();
									}
								}
							}, key, "222");

				}

			}
		});
	}

	@Test
	public void testHSetAndHGet() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				shardedNedis.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				int repeats = 20;
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					String field = "field-" + i;
					String value = "value-" + i;
					final boolean isLast = i == repeats - 1;
					shardedNedis.hSet(
							new ResponseCallback<ShardedResponse<Boolean>>() {

								@Override
								public void failed(Throwable cause) {
									fail(cause);
								}

								@Override
								public void done(ShardedResponse<Boolean> result) {
									System.out.println(result);
									assertEquals(true, result.getResult());
								}
							}, key, field, value);
					Thread.sleep(CMD_PAUSE_TIME);
					shardedNedis.hGet(
							new ResponseCallback<ShardedResponse<String>>() {

								@Override
								public void done(ShardedResponse<String> result) {
									System.out.println("result:" + result);
									if (isLast) {
										controller.countDown();
									}
								}

								@Override
								public void failed(Throwable cause) {
									fail(cause);
									if (isLast) {
										controller.countDown();
									}
								}
							}, key, field);
				}

			}
		});
	}
}
