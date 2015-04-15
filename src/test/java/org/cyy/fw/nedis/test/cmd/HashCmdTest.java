package org.cyy.fw.nedis.test.cmd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Map.Entry;

import org.cyy.fw.nedis.KeyValuePair;
import org.cyy.fw.nedis.ResponseCallback;
import org.cyy.fw.nedis.ScanParams;
import org.cyy.fw.nedis.ScanResult;
import org.cyy.fw.nedis.util.NedisException;
import org.junit.Test;

public class HashCmdTest extends BaseCmdTest {

	@Test
	public void testHSet() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(new ResponseCallback<Boolean>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Boolean result) {
						assertTrue(result);
					}
				}, "hkey1", "field1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(new ResponseCallback<Boolean>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Boolean result) {
						assertTrue(!result);
					}
				}, "hkey1", "field1", "value1");

				Thread.sleep(CMD_PAUSE_TIME);
				client.hSet(new ResponseCallback<Boolean>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}

					@Override
					public void done(Boolean result) {
						assertTrue(result);
						controller.countDown();
					}
				}, "hkey1", "field2", "value2");

			}
		});
	}

	@Test
	public void testHSetNX() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSetNX(new ResponseCallback<Boolean>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Boolean result) {
						assertTrue(result);
					}
				}, "hkey1", "field1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSetNX(new ResponseCallback<Boolean>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}

					@Override
					public void done(Boolean result) {
						assertTrue(!result);
						controller.countDown();
					}
				}, "hkey1", "field1", "value2");

			}
		});
	}

	@Test
	public void testHGet() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hGet(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("null", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1", "field1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hGet(new ResponseCallback<String>() {

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
				}, "hkey1", "field1");

			}
		});
	}

	@Test
	public void testHDel() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hGet(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("value1", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1", "field1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hDel(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(1), result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1", "field1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hGet(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("null", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1", "field1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hDel(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(0), result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "hkey1", "field1");

			}
		});
	}

	@Test
	public void testHExist() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hExist(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(false, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1", "hfiled1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "hfiled1", "hvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hExist(new ResponseCallback<Boolean>() {

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
				}, "hkey1", "hfiled1");
			}
		});
	}

	@Test
	public void testHGetAll() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field2", "value2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field3", "value3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hGetAll(new ResponseCallback<Map<String, String>>() {

					@Override
					public void done(Map<String, String> result) {
						for (Entry<String, String> entry : result.entrySet()) {
							System.out.println(entry.getKey());
							System.out.println(entry.getValue());
						}
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
	public void testHIncrBy() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hIncrBy(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(200, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1", "hfield1", 200);
				client.hSet(null, "hkey1", "hfield1", "2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hIncrBy(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(202, result.longValue());
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "hkey1", "hfield1", 200);

			}
		});
	}

	@Test
	public void testHIncrByFloat() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hIncrByFloat(new ResponseCallback<Double>() {

					@Override
					public void done(Double result) {
						assertEquals(200.01, result.doubleValue(), 0.00001);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1", "hfield1", 200.01);
				client.hSet(null, "hkey1", "hfield1", "2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hIncrByFloat(new ResponseCallback<Double>() {

					@Override
					public void done(Double result) {
						assertEquals(202.11, result.doubleValue(), 0.00001);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "hkey1", "hfield1", 200.11);

			}
		});
	}

	@Test
	public void testHKeys() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hKeys(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(0, result.length);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field2", "value2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field3", "value3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hKeys(new ResponseCallback<String[]>() {

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
				}, "hkey1");

			}
		});
	}

	@Test
	public void testHVals() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hVals(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(0, result.length);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field2", "value2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field3", "value3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hVals(new ResponseCallback<String[]>() {

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
				}, "hkey1");

			}
		});
	}

	@Test
	public void testHLen() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hLen(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(0, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field2", "value2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "hkey1", "field3", "value3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hLen(new ResponseCallback<Long>() {

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
				}, "hkey1");

			}
		});
	}

	@Test
	public void testHMSet() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hMSet(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1", new KeyValuePair("hfield1", "hvalue1"),
						new KeyValuePair("hfield2", "hvalue2"),
						new KeyValuePair("hfield3", "hvalue3"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.hGet(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("hvalue1", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1", "hfield1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hGet(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("hvalue2", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1", "hfield2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hGet(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("hvalue3", result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "hkey1", "hfield3");

			}
		});
	}

	@Test
	public void testHMGet() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hMSet(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertEquals("OK", result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "hkey1", new KeyValuePair("hfield1", "hvalue1"),
						new KeyValuePair("hfield2", "hvalue2"),
						new KeyValuePair("hfield3", "hvalue3"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.hMGet(new ResponseCallback<String[]>() {

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
				}, "hkey1", "hfield1", "hfield2", "hfield3", "hfield4");

			}
		});
	}

	@Test
	public void doHScan() {
		doCmdTest(new TestAction() {

			String cursor = "0";

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field2", "value2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field3", "value3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field4", "value4");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field5", "value5");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field6", "value6");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field7", "value7");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field8", "value8");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field9", "value9");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field10", "value10");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field11", "value11");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field12", "value12");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field13", "value13");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field14", "value14");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field15", "value15");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field16", "value16");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field17", "value17");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field18", "value18");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field19", "value19");
				Thread.sleep(CMD_PAUSE_TIME);

				client.hSet(null, "key1", "field20", "value20");
				Thread.sleep(CMD_PAUSE_TIME);

				final int count = 5;
				client.hScan(
						new ResponseCallback<ScanResult<Map.Entry<String, String>>>() {

							@Override
							public void done(
									ScanResult<Map.Entry<String, String>> result) {
								cursor = result.getCursor();
								System.out.println("cursor:" + cursor);
								System.out.println("result.size:"
										+ result.getResult().size());
								for (Map.Entry<String, String> r : result
										.getResult()) {
									System.out.println(r.getKey());
									System.out.println(r.getValue());
								}
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
							}
						}, "key1", cursor, new ScanParams().count(count));
				Thread.sleep(CMD_PAUSE_TIME);

				client.hScan(
						new ResponseCallback<ScanResult<Map.Entry<String, String>>>() {

							@Override
							public void done(
									ScanResult<Map.Entry<String, String>> result) {
								cursor = result.getCursor();
								System.out.println("cursor:" + cursor);
								System.out.println("result.size:"
										+ result.getResult().size());
								for (Map.Entry<String, String> r : result
										.getResult()) {
									System.out.println(r.getKey());
									System.out.println(r.getValue());
								}
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
							}
						}, "key1", cursor, new ScanParams().count(count));
				Thread.sleep(CMD_PAUSE_TIME);

				client.hScan(
						new ResponseCallback<ScanResult<Map.Entry<String, String>>>() {

							@Override
							public void done(
									ScanResult<Map.Entry<String, String>> result) {
								cursor = result.getCursor();
								System.out.println("cursor:" + cursor);
								System.out.println("result.size:"
										+ result.getResult().size());
								for (Map.Entry<String, String> r : result
										.getResult()) {
									System.out.println(r.getKey());
									System.out.println(r.getValue());
								}
								controller.countDown();
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
								controller.countDown();
							}
						}, "key1", cursor, new ScanParams().count(count));
			}
		});
	}
}
