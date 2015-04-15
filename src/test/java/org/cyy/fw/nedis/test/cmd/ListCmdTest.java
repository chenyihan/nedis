package org.cyy.fw.nedis.test.cmd;

import static org.junit.Assert.assertEquals;

import org.cyy.fw.nedis.RedisProtocol;
import org.cyy.fw.nedis.ResponseCallback;
import org.cyy.fw.nedis.util.NedisException;
import org.junit.Test;

public class ListCmdTest extends BaseCmdTest {

	@Test
	public void testLPush() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(1), result);
					}
				}, "llkey1", "llvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(2), result);
						controller.countDown();
					}
				}, "llkey1", "llvalue1", "llvalue2");

			}
		});
	}

	@Test
	public void testLPushX() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPushX(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(0), result);
					}
				}, "llkey1", "llvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(2), result);
					}
				}, "llkey1", "llvalue1", "llvalue2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPushX(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(3), result);
						controller.countDown();
					}
				}, "llkey1", "llvalue3");
			}
		});
	}

	@Test
	public void testLPop() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "llkey1", "llvalue1", "llvalue2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPop(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("llvalue2", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "llkey1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPop(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("llvalue1", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "llkey1");

			}
		});
	}

	@Test
	public void testRPush() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.rPush(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(1), result);
					}
				}, "lrkey1", "lrvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.rPush(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(2), result);
						controller.countDown();
					}
				}, "lrkey1", "lrvalue1", "lrvalue2");

			}
		});
	}

	@Test
	public void testRPushX() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.rPushX(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(0), result);
					}
				}, "llkey1", "llvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.rPush(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(2), result);
					}
				}, "llkey1", "llvalue1", "llvalue2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.rPushX(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(3), result);
						controller.countDown();
					}
				}, "llkey1", "llvalue3");
			}
		});
	}

	@Test
	public void testRPop() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.rPush(null, "lrkey1", "lrvalue1", "lrvalue2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.rPop(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("lrvalue2", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "lrkey1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.rPop(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("lrvalue1", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "lrkey1");

			}
		});
	}

	@Test
	public void testBLPop() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.blPop(new ResponseCallback<String[]>() {

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
				}, 30, "llkey1");

			}
		});
	}

	@Test
	public void testBRPop() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.brPop(new ResponseCallback<String[]>() {

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
				}, 30, "llkey1", "llkey2");

			}
		});
	}

	@Test
	public void testLRange() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "llkey1", "llvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "llkey1", "llvalue1", "llvalue2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lRange(new ResponseCallback<String[]>() {

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
				}, "llkey1", 0, -1);

			}
		});
	}

	@Test
	public void testRPOPLPUSH() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "llkey1", "llvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "llkey2", "llvalue2", "llvalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.rPopLPush(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("llvalue1", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "llkey1", "llkey2");

			}
		});
	}

	@Test
	public void testBRPOPLPUSH() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "llkey1", "llvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "llkey2", "llvalue2", "llvalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.bRPopLPush(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("llvalue1", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "llkey1", "llkey2", 30);

			}
		});
	}

	@Test
	public void testLIndex() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "llkey1", "llvalue1", "llvalue2", "llvalue3");
				Thread.sleep(CMD_PAUSE_TIME);
				client.lIndex(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("llvalue1", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "llkey1", 2);

				client.lIndex(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("llvalue3", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "llkey1", 0);

				client.lIndex(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals(RedisProtocol.NULL, result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "llkey1", 4);

			}
		});
	}

	@Test
	public void testLInsert() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "llkey1", "llvalue1", "llvalue2", "llvalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lInsert(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(0, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "llkey2", "llvalue4", "llvalue5", true);

				Thread.sleep(CMD_PAUSE_TIME);

				client.lInsert(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(-1, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "llkey1", "llvalue4", "llvalue5", true);

				Thread.sleep(CMD_PAUSE_TIME);

				client.lInsert(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(4, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "llkey1", "llvalue4", "llvalue2", true);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lRange(new ResponseCallback<String[]>() {

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
				}, "llkey1", 0, -1);

			}
		});
	}

	@Test
	public void testLLen() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lLen(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(0, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "llkey1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "llkey1", "llvalue1", "llvalue2", "llvalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lLen(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(3, result.longValue());
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "llkey1");

			}
		});
	}

	@Test
	public void testLREM() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(2), result);
					}
				}, "llkey1", "llvalue1", "llvalue2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lREM(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(1), result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "llkey1", "llvalue1", 0);

			}
		});
	}

	@Test
	public void testLSet() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(2), result);
					}
				}, "llkey1", "llvalue1", "llvalue2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lSet(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "llkey1", 4, "llvalue4");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lSet(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "llkey1", 2, "llvalue4");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lRange(new ResponseCallback<String[]>() {

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
				}, "llkey1", 0, -1);

			}
		});
	}

	@Test
	public void testLTrim() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lPush(null, "llkey1", "llvalue1", "llvalue2",
						"llvalue3", "llvalue4");
				Thread.sleep(CMD_PAUSE_TIME);

				client.lTrim(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "llkey1", 1, 2);
				Thread.sleep(CMD_PAUSE_TIME);

				client.lRange(new ResponseCallback<String[]>() {

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
				}, "llkey1", 0, -1);

			}
		});
	}
}
