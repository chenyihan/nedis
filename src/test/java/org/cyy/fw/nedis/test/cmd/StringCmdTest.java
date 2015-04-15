package org.cyy.fw.nedis.test.cmd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.cyy.fw.nedis.BitOP;
import org.cyy.fw.nedis.KeyValuePair;
import org.cyy.fw.nedis.RedisProtocol;
import org.cyy.fw.nedis.ResponseCallback;
import org.cyy.fw.nedis.util.NedisException;
import org.junit.Test;

public class StringCmdTest extends BaseCmdTest {

	@Test
	public void testSet() {

		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				client.set(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", "value1");

				Thread.sleep(CMD_PAUSE_TIME);
				client.set(new ResponseCallback<String>() {

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
				}, "key1", "value2");

			}
		});

	}

	@Test
	public void testGet() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				client.set(null, "key1", "value1");
				ResponseCallback<String> respCallback = new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("value1", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				};
				Thread.sleep(CMD_PAUSE_TIME);
				client.get(respCallback, "key1");

				respCallback = new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("null", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				};

				client.get(respCallback, "key2");

			}
		});

	}

	@Test
	public void testDel() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.del(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(0), result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.del(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(1), result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");

				Thread.sleep(CMD_PAUSE_TIME);
				client.set(null, "key1", "value1");
				client.set(null, "key2", "value2");

				Thread.sleep(CMD_PAUSE_TIME);
				client.del(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(2), result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key1", "key2");

			}
		});

	}

	@Test
	public void testKeys() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				client.keys(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(0, result.length);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "*");

				client.set(null, "key1", "value1");
				client.set(null, "key2", "value2");

				client.keys(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(2, result.length);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key*");

			}
		});

	}

	@Test
	public void testExists() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				client.exists(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertTrue(!result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);
				client.exists(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertTrue(result);
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
	public void testAppend() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);
				client.append(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf("111".length()), result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", "111");

				Thread.sleep(CMD_PAUSE_TIME);
				client.append(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf("111222".length()), result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key1", "222");

			}
		});

	}

	@Test
	public void testBit() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.setBit(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(0, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", 10086, 1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.getBit(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(1, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", 10086);

				Thread.sleep(CMD_PAUSE_TIME);

				client.getBit(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(0, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", 100);

				client.bitCount(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(1, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");

				client.bitCount(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(0, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", 0, 100);
				Thread.sleep(CMD_PAUSE_TIME);

				client.setBit(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(0, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", 100, 1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.bitCount(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(1, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", 0, 100);
				Thread.sleep(CMD_PAUSE_TIME);
				client.bitCount(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(2, result.longValue());
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
	public void testBitOP() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.setBit(null, "key1", 0, 1);
				Thread.sleep(CMD_PAUSE_TIME);
				client.setBit(null, "key1", 3, 1);
				Thread.sleep(CMD_PAUSE_TIME);
				client.setBit(null, "key2", 0, 1);
				Thread.sleep(CMD_PAUSE_TIME);
				client.setBit(null, "key2", 1, 1);
				Thread.sleep(CMD_PAUSE_TIME);
				client.setBit(null, "key2", 3, 1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.bitop(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						System.out.println("result:" + result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, BitOP.AND, "key3", "key1", "key2");

				Thread.sleep(CMD_PAUSE_TIME);

				client.bitop(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						System.out.println("result:" + result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, BitOP.OR, "key4", "key1", "key2");

				Thread.sleep(CMD_PAUSE_TIME);

				client.bitop(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						System.out.println("result:" + result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, BitOP.XOR, "key5", "key1", "key2");

				Thread.sleep(CMD_PAUSE_TIME);

				client.bitop(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						System.out.println("result:" + result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, BitOP.NOT, "key6", "key1");
			}
		});
	}

	@Test
	public void testIncr() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.incr(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(1, result.longValue());
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("1", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				client.set(null, "key2", "43");
				Thread.sleep(CMD_PAUSE_TIME);

				client.incr(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(44, result.longValue());
					}
				}, "key2");

				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("44", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key2");
			}
		});
	}

	@Test
	public void testIncrBy() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.incrBy(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(2, result.longValue());
					}
				}, "key1", 2);
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("2", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				client.set(null, "key2", "43");
				Thread.sleep(CMD_PAUSE_TIME);

				client.incrBy(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(46, result.longValue());
					}
				}, "key2", 3);

				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("46", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key2");
			}
		});
	}

	@Test
	public void testIncrByFloat() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.incrByFloat(new ResponseCallback<Double>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Double result) {
						assertEquals(2.1, result.doubleValue(), 0.00001);
					}
				}, "key1", 2.1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("2.1", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				client.set(null, "key2", "43");
				Thread.sleep(CMD_PAUSE_TIME);

				client.incrByFloat(new ResponseCallback<Double>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Double result) {
						assertEquals(46.444444, result.doubleValue(),
								0.000000001);
					}
				}, "key2", 3.444444);

				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("46.444444", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key2");
			}
		});
	}

	@Test
	public void testDecr() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.decr(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(-1, result.longValue());
					}
				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("-1", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				client.set(null, "key2", "43");
				Thread.sleep(CMD_PAUSE_TIME);

				client.decr(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(42, result.longValue());
					}
				}, "key2");

				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("42", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key2");
			}
		});
	}

	@Test
	public void testDecrBy() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.decrBy(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(-2, result.longValue());
					}
				}, "key1", 2);
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("-2", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				client.set(null, "key2", "43");
				Thread.sleep(CMD_PAUSE_TIME);

				client.decrBy(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(40, result.longValue());
					}
				}, "key2", 3);

				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("40", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key2");
			}
		});
	}

	@Test
	public void testGetRange() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "hello, my friend");
				Thread.sleep(CMD_PAUSE_TIME);

				client.getRange(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("hello", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", 0, 4);
				Thread.sleep(CMD_PAUSE_TIME);

				client.getRange(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", -1, -5);
				Thread.sleep(CMD_PAUSE_TIME);

				client.getRange(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("end", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", -3, -1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.getRange(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("hello, my friend", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", 0, -1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.getRange(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("hello, my friend", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key1", 0, 1008611);
			}
		});
	}

	@Test
	public void testGetSet() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.getSet(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertTrue(RedisProtocol.isNull(result));
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("value1", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

				}, "key1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.getSet(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("value1", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", "value2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("value2", result);
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
	public void testMSetAndGet() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.mSet(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, new KeyValuePair("key1", "value1"), new KeyValuePair("key2",
						"value2"), new KeyValuePair("key3", "value3"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("value1", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("value2", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key2");
				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("value3", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key3");

				client.mGet(new ResponseCallback<String[]>() {

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
				}, "key1", "key2", "key3", "key4");

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.mGet(new ResponseCallback<String[]>() {

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

				client.mSetNX(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(false, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, new KeyValuePair("key1", "value1"), new KeyValuePair("key4",
						"value4"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.mSetNX(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(true, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, new KeyValuePair("key5", "value5"), new KeyValuePair("key4",
						"value4"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.setNX(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(false, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key5", "value5");

				Thread.sleep(CMD_PAUSE_TIME);

				client.setNX(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(true, result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key6", "value6");

			}
		});
	}

	@Test
	public void testSetEX() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.setEX(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", 100, "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("value1", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				client.ttl(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						System.out.println("result:" + result);
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
	public void testPSetEX() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.pSetEX(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", 100000, "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("value1", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				client.pTTL(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						System.out.println("result:" + result);
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
	public void testSetRange() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "hello world");
				Thread.sleep(CMD_PAUSE_TIME);

				client.setRange(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(11, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", 6, "Redis");
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("hello Redis", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");

				client.setRange(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(11, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key2", 6, "Redis");
				Thread.sleep(CMD_PAUSE_TIME);

				client.get(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						System.out.println("result:" + result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key2");

			}
		});
	}

	@Test
	public void testStrLen() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.set(null, "key1", "Hello world");
				Thread.sleep(CMD_PAUSE_TIME);

				client.strLen(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(11, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1");
				client.strLen(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(0, result.longValue());
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "key2");
			}

		});
	}
}
