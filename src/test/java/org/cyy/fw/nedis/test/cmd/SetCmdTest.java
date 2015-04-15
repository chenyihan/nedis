package org.cyy.fw.nedis.test.cmd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.cyy.fw.nedis.ResponseCallback;
import org.cyy.fw.nedis.ScanParams;
import org.cyy.fw.nedis.ScanResult;
import org.cyy.fw.nedis.util.NedisException;
import org.junit.Test;

public class SetCmdTest extends BaseCmdTest {

	@Test
	public void testSAdd() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(1), result);
					}
				}, "skey1", "svalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(0), result);
					}
				}, "skey1", "svalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(new ResponseCallback<Long>() {

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
				}, "skey1", "svalue2", "svalue3");

			}
		});
	}

	@Test
	public void testSPop() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sPop(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertNotNull(result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sPop(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertNotNull(result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sPop(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertNotNull(result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sPop(new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						assertResultNull(result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "skey1");

			}
		});
	}

	@Test
	public void testSRandMember() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sRandMember(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(1, result.length);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sRandMember(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(2, result.length);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1", 2);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sRandMember(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(3, result.length);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1", 3);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sRandMember(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(3, result.length);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1", 4);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sRandMember(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(2, result.length);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1", -2);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sRandMember(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(3, result.length);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1", -3);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sRandMember(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(4, result.length);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "skey1", -4);

			}
		});
	}

	@Test
	public void testSCard() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1", "svalue2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sCard(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(3), result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "skey1");

			}
		});
	}

	@Test
	public void testSDiff() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey2", "svalue4", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sDiff(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(1, result.length);
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
				}, "skey1", "skey2");

			}
		});
	}

	@Test
	public void testSDiffStore() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey2", "svalue4", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sDiffStore(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(1, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey3", "skey1", "skey2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sMembers(new ResponseCallback<String[]>() {

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
				}, "skey3");
			}
		});
	}

	@Test
	public void testSInter() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey2", "svalue4", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sInter(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(2, result.length);
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
				}, "skey1", "skey2");

			}
		});
	}

	@Test
	public void testSInterStore() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey2", "svalue4", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sInterStore(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(2, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey3", "skey1", "skey2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sMembers(new ResponseCallback<String[]>() {

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
				}, "skey3");
			}
		});
	}

	@Test
	public void testSisMember() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey2", "svalue4", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sisMember(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(false, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1", "skey2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sisMember(new ResponseCallback<Boolean>() {

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
				}, "skey1", "svalue1");
			}
		});
	}

	@Test
	public void testSMove() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sMembers(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(0, result.length);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sMove(new ResponseCallback<Boolean>() {

					@Override
					public void done(Boolean result) {
						assertEquals(true, result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1", "skey2", "svalue2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sMembers(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(2, result.length);
						for (String r : result) {
							System.out.println(r);
						}
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1");
				client.sMembers(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(1, result.length);
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
				}, "skey2");
			}
		});
	}

	@Test
	public void testSREM() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sRem(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(2, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey1", "svalue1", "svalue2", "svalue4");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sMembers(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(1, result.length);
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
				}, "skey1");
			}
		});
	}

	@Test
	public void testSUnion() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey2", "svalue4", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sUnion(new ResponseCallback<String[]>() {

					@Override
					public void done(String[] result) {
						assertEquals(4, result.length);
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
				}, "skey1", "skey2");

			}
		});
	}

	@Test
	public void testSUnionStore() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey1", "svalue1", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "skey2", "svalue4", "svalue2", "svalue3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sUnionStore(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(4, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "skey3", "skey1", "skey2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sMembers(new ResponseCallback<String[]>() {

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
				}, "skey3");
			}
		});
	}

	@Test
	public void testSScan() {
		doCmdTest(new TestAction() {

			String cursor = "0";

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value4");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value5");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value6");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value7");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value8");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value9");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value10");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value11");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value12");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value13");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value14");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value15");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value16");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value17");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value18");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value19");
				Thread.sleep(CMD_PAUSE_TIME);

				client.sAdd(null, "key1", "value20");
				Thread.sleep(CMD_PAUSE_TIME);

				final int count = 5;
				client.sScan(new ResponseCallback<ScanResult<String>>() {

					@Override
					public void done(ScanResult<String> result) {
						cursor = result.getCursor();
						System.out.println("cursor:" + cursor);
						System.out.println("result.size:"
								+ result.getResult().size());
						for (String r : result.getResult()) {
							System.out.println(r);
						}
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", cursor, new ScanParams().count(count));
				Thread.sleep(CMD_PAUSE_TIME);

				client.sScan(new ResponseCallback<ScanResult<String>>() {

					@Override
					public void done(ScanResult<String> result) {
						cursor = result.getCursor();
						System.out.println("cursor:" + cursor);
						System.out.println("result.size:"
								+ result.getResult().size());
						for (String r : result.getResult()) {
							System.out.println(r);
						}
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "key1", cursor, new ScanParams().count(count));
				Thread.sleep(CMD_PAUSE_TIME);

				client.sScan(new ResponseCallback<ScanResult<String>>() {

					@Override
					public void done(ScanResult<String> result) {
						cursor = result.getCursor();
						System.out.println("cursor:" + cursor);
						System.out.println("result.size:"
								+ result.getResult().size());
						for (String r : result.getResult()) {
							System.out.println(r);
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
