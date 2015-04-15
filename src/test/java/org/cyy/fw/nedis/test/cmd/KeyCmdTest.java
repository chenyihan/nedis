package org.cyy.fw.nedis.test.cmd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.cyy.fw.nedis.NedisClient;
import org.cyy.fw.nedis.NedisClientBuilder;
import org.cyy.fw.nedis.RedisProtocol;
import org.cyy.fw.nedis.ResponseCallback;
import org.cyy.fw.nedis.ScanParams;
import org.cyy.fw.nedis.ScanResult;
import org.cyy.fw.nedis.SortingParams;
import org.cyy.fw.nedis.util.NedisException;
import org.cyy.fw.nedis.util.TextEncoder;
import org.junit.Test;

public class KeyCmdTest extends BaseCmdTest {

	@Test
	public void testDump() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				client.dump(new ResponseCallback<byte[]>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(byte[] result) {
						assertTrue(RedisProtocol.isNull(TextEncoder
								.decode(result)));
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);
				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);
				client.dump(new ResponseCallback<byte[]>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(byte[] result) {
						assertTrue(!RedisProtocol.isNull(TextEncoder
								.decode(result)));
						System.out.println("result:" + result);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);
				client.dump(new ResponseCallback<byte[]>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(byte[] result) {
						assertTrue(RedisProtocol.isNull(TextEncoder
								.decode(result)));
						controller.countDown();

					}
				}, "key2");
			}
		});
	}

	@Test
	public void testRestore() {
		doCmdTest(new TestAction() {
			private byte[] deValue;

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);
				client.dump(new ResponseCallback<byte[]>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(byte[] result) {
						assertTrue(!RedisProtocol.isNull(TextEncoder
								.decode(result)));
						System.out.println("result:" + result);
						deValue = result;
					}
				}, "key1");

				Thread.sleep(CMD_PAUSE_TIME);

				client.restore(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key2", deValue);
				Thread.sleep(CMD_PAUSE_TIME);

				client.restore(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key3", 0, deValue, false);
			}
		});
	}

	@Test
	public void testExpire() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				final long seconds = 10;

				client.expire(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(false, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);

					}
				}, "key1", seconds);
				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.expire(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(true, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);

					}
				}, "key1", seconds);

				Thread.sleep(CMD_PAUSE_TIME);
				client.ttl(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertTrue(result <= seconds);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key1");
			}
		});
	}

	@Test
	public void testExpireAt() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				final long ts = 2355292000l;
				client.expireAt(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(false, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);

					}
				}, "key1", ts);
				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.expireAt(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(true, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);

					}
				}, "key1", ts);
				Thread.sleep(CMD_PAUSE_TIME);
				client.ttl(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						System.out.println(ts);
						assertTrue(result <= ts);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key1");
			}
		});
	}

	@Test
	public void testPExpire() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				final long millSeconds = 10000;

				client.pExpire(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(false, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);

					}
				}, "key1", millSeconds);
				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.pExpire(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(true, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);

					}
				}, "key1", millSeconds);
				Thread.sleep(CMD_PAUSE_TIME);
				client.pTTL(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertTrue(result <= millSeconds);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();

					}
				}, "key1");
			}
		});
	}

	@Test
	public void testPExpireAt() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				final long ts = 2355292000000l;
				client.pExpireAt(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(false, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);

					}
				}, "key1", ts);
				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.pExpireAt(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(true, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);

					}
				}, "key1", ts);

				Thread.sleep(CMD_PAUSE_TIME);
				client.pTTL(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertTrue(result <= ts);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();

					}
				}, "key1");
			}
		});
	}

	@Test
	public void testTTL() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				client.ttl(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(-2, result.longValue());

					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.ttl(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(-1, result.longValue());

					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				final long seconds = 10;
				client.expire(null, "key1", seconds);
				Thread.sleep(CMD_PAUSE_TIME);

				client.ttl(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertTrue(result <= seconds);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key1");
			}
		});
	}

	@Test
	public void testPTTL() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				client.pTTL(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(-2, result.longValue());

					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.pTTL(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(-1, result.longValue());

					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				final long ts = 2355292000000l;
				client.pExpire(null, "key1", ts);
				Thread.sleep(CMD_PAUSE_TIME);

				client.pTTL(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertTrue(result <= ts);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key1");
			}
		});
	}

	@Test
	public void testObject() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.objectRefcount(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(1, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.objectIdletime(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						System.out.println("ilde time:" + result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(null, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.objectIdletime(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						System.out.println("ilde time:" + result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.objectEncoding(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("raw", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "158201332432423");
				Thread.sleep(CMD_PAUSE_TIME);

				client.objectEncoding(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("raw", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "20");
				Thread.sleep(CMD_PAUSE_TIME);

				client.objectEncoding(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("int", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key1");
			}
		});
	}

	@Test
	public void testPersist() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.expire(null, "key1", 10);
				Thread.sleep(CMD_PAUSE_TIME);

				client.ttl(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertTrue(0 < result && result <= 10);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.persist(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(true, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);
				client.ttl(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(-1, result.longValue());
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key1");
			}
		});
	}

	@Test
	public void testRandomKey() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.randomKey(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertTrue(RedisProtocol.isNull(result));
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				});
				Thread.sleep(CMD_PAUSE_TIME);
				client.set(null, "key1", "value1");
				client.set(null, "key2", "value2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.randomKey(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertTrue(!RedisProtocol.isNull(result));
						assertTrue("key1".equals(result)
								|| "key2".equals(result));
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				});
			}
		});
	}

	@Test
	public void testRename() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.rename(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						System.out.println(result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", "key2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.rename(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", "key2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.exists(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(false, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.exists(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(true, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key2");

				client.set(null, "key3", "value3");
				client.set(null, "key4", "value4");
				Thread.sleep(CMD_PAUSE_TIME);
				client.rename(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key3", "key4");

				Thread.sleep(CMD_PAUSE_TIME);
				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertTrue(RedisProtocol.isNull(result));
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("value3", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key4");

			}
		});
	}

	@Test
	public void testRenameNX() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.renameNX(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						// TODO Auto-generated method stub

					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", "key2");

				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "value1");
				client.set(null, "key2", "value2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.renameNX(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(false, result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key1", "key2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.renameNX(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(true, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", "key3");
			}
		});
	}

	@Test
	public void testMigrate() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				String destHost = "192.168.1.105";
				int destPort = 6379;
				NedisClient destClient = new NedisClientBuilder()
						.setServerHost(destHost).setPort(destPort)
						.setConnectTimeoutMills(connectTimeoutMills)
						.setConnectionPoolSize(5).build();
				destClient.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.migrate(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", destHost, destPort, 0, 10l);
				Thread.sleep(CMD_PAUSE_TIME);
				Thread.sleep(CMD_PAUSE_TIME);

				client.exists(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(false, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				destClient.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("value1", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key1");
			}
		});
	}

	@Test
	public void testType() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.type(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("string", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "llkey1", "llvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.type(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("list", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "llkey1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.type(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("set", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zkey1", 1.0, "zvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.type(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("zset", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zkey1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "hfield1", "hvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.type(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("hash", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "hkey1");

			}
		});
	}

	@Test
	public void doScan() {
		doCmdTest(new TestAction() {

			String cursor = "0";

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key2", "value2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key3", "value3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key4", "value4");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key5", "value5");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key6", "value6");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key7", "value7");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key8", "value8");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key9", "value9");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key10", "value10");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key11", "value11");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key12", "value12");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key13", "value13");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key14", "value14");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key15", "value15");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key16", "value16");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key17", "value17");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key18", "value18");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key19", "value19");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key20", "value20");
				Thread.sleep(CMD_PAUSE_TIME);

				final int count = 5;
				client.scan(new ResponseCallback<ScanResult<String>>() {

					@Override
					public void done(ScanResult<String> result) {
						cursor = result.getCursor();
						System.out.println("cursor:" + cursor);
						System.out.println("result.size:"
								+ result.getResult().size());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, cursor, new ScanParams().count(count));
				Thread.sleep(CMD_PAUSE_TIME);

				client.scan(new ResponseCallback<ScanResult<String>>() {

					@Override
					public void done(ScanResult<String> result) {
						cursor = result.getCursor();
						System.out.println("cursor:" + cursor);
						System.out.println("result.size:"
								+ result.getResult().size());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, cursor, new ScanParams().count(count));
				Thread.sleep(CMD_PAUSE_TIME);

				client.scan(new ResponseCallback<ScanResult<String>>() {

					@Override
					public void done(ScanResult<String> result) {
						cursor = result.getCursor();
						System.out.println("cursor:" + cursor);
						System.out.println("result.size:"
								+ result.getResult().size());
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, cursor, new ScanParams().count(count));
			}
		});
	}

	@Test
	public void testSort() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "key1", "25", new String[] { "3", "5", "4",
						"55", "34", "15", "2" });
				Thread.sleep(CMD_PAUSE_TIME);

				client.sort(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						for (String r : result) {
							System.out.println(r);
						}
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sort(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						for (String r : result) {
							System.out.println(r);
						}
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", new SortingParams().desc());

				Thread.sleep(CMD_PAUSE_TIME);

				client.sort(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						for (String r : result) {
							System.out.println(r);
						}
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", new SortingParams().alpha());

				Thread.sleep(CMD_PAUSE_TIME);

				client.sort(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						for (String r : result) {
							System.out.println(r);
						}
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", new SortingParams().limit(2, 4));

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "key1", "25", new String[] { "3", "5", "4",
						"55", "34", "15", "2" });
				Thread.sleep(CMD_PAUSE_TIME);

				client.sort(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						for (String r : result) {
							System.out.println(r);
						}
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key1", "key2");
				Thread.sleep(CMD_PAUSE_TIME);

				// TODO lrange key2 0 -1

			}
		});
	}

	@Test
	public void doMove() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.move(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(true, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", 1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals(RedisProtocol.NULL, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.select(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, 1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("value1", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key1");
			}
		});
	}
}
