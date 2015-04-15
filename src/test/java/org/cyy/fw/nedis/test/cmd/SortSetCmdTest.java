package org.cyy.fw.nedis.test.cmd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.cyy.fw.nedis.ResponseCallback;
import org.cyy.fw.nedis.ScanParams;
import org.cyy.fw.nedis.ScanResult;
import org.cyy.fw.nedis.ScoreMemberPair;
import org.cyy.fw.nedis.SortedSetParams;
import org.cyy.fw.nedis.SortedSetParams.Aggregate;
import org.cyy.fw.nedis.util.NedisException;
import org.junit.Test;

public class SortSetCmdTest extends BaseCmdTest {

	@Test
	public void testZAdd() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(new ResponseCallback<Long>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}

					@Override
					public void done(Long result) {
						assertEquals(Long.valueOf(1), result);
					}
				}, "zskey1", 2, "zsvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(new ResponseCallback<Long>() {

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
				}, "zskey1", new ScoreMemberPair(1, "zsvalue2"),
						new ScoreMemberPair(4, "zsvalue3"));

			}
		});
	}

	@Test
	public void testZScore() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1",
						new ScoreMemberPair(2.1, "zsvalue1"),
						new ScoreMemberPair(1.2, "zsvalue2"),
						new ScoreMemberPair(4.5, "zsvalue3"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zScore(new ResponseCallback<Double>() {

					@Override
					public void done(Double result) {
						assertEquals(Double.valueOf(2.1), result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", "zsvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zScore(new ResponseCallback<Double>() {

					@Override
					public void done(Double result) {
						assertEquals(Double.valueOf(1.2), result);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", "zsvalue2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zScore(new ResponseCallback<Double>() {

					@Override
					public void done(Double result) {
						assertEquals(Double.valueOf(4.5), result);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "zskey1", "zsvalue3");

			}
		});
	}

	@Test
	public void testZCard() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1", new ScoreMemberPair(1, "zsvalue2"),
						new ScoreMemberPair(4, "zsvalue3"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zCard(new ResponseCallback<Long>() {

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
				}, "zskey1");

			}

		});
	}

	@Test
	public void testZCount() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1", new ScoreMemberPair(1, "zsvalue1"),
						new ScoreMemberPair(4, "zsvalue2"),
						new ScoreMemberPair(2.1, "zsvalue3"),
						new ScoreMemberPair(3.3, "zsvalue4"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zCount(new ResponseCallback<Long>() {

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
				}, "zskey1", 0.1, 2.8);

			}

		});
	}

	@Test
	public void testZIncrBy() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zIncrBy(new ResponseCallback<Double>() {

					@Override
					public void done(Double result) {
						assertEquals(1.1, result, 0.00001);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", "zsvalue1", 1.1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zIncrBy(new ResponseCallback<Double>() {

					@Override
					public void done(Double result) {
						assertEquals(2.1, result, 0.00001);
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", "zsvalue2", 2.1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zIncrBy(new ResponseCallback<Double>() {

					@Override
					public void done(Double result) {
						assertEquals(4.2, result, 0.00001);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "zskey1", "zsvalue2", 2.1);

			}

		});
	}

	@Test
	public void testZRange() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1", 2, "zsvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1", new ScoreMemberPair(1, "zsvalue2"),
						new ScoreMemberPair(4, "zsvalue3"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRange(new ResponseCallback<String[]>() {

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
				}, "zskey1", 0, -1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRangeWithScores(
						new ResponseCallback<ScoreMemberPair[]>() {

							@Override
							public void done(ScoreMemberPair[] result) {
								for (ScoreMemberPair p : result) {
									System.out.println(p);
								}
								controller.countDown();
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
								controller.countDown();
							}
						}, "zskey1", 0, -1);

			}
		});
	}

	@Test
	public void testZRangeByScore() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1", new ScoreMemberPair(2, "zsvalue1"),
						new ScoreMemberPair(1, "zsvalue2"),
						new ScoreMemberPair(4, "zsvalue3"),
						new ScoreMemberPair(7, "zsvalue4"),
						new ScoreMemberPair(8, "zsvalue5"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRangeByScore(new ResponseCallback<String[]>() {

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
				}, "zskey1", 4, 8);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRangeByScore(new ResponseCallback<String[]>() {

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
				}, "zskey1", 4, 8, 2, 1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRevRangeWithScores(
						new ResponseCallback<ScoreMemberPair[]>() {

							@Override
							public void done(ScoreMemberPair[] result) {
								for (ScoreMemberPair p : result) {
									System.out.println(p);
								}
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
							}
						}, "zskey1", 4, 8);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRangeByScoreWithScores(
						new ResponseCallback<ScoreMemberPair[]>() {

							@Override
							public void done(ScoreMemberPair[] result) {
								for (ScoreMemberPair p : result) {
									System.out.println(p);
								}
								controller.countDown();
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
								controller.countDown();
							}
						}, "zskey1", 4, 8, 2, 1);
			}
		});
	}

	@Test
	public void testZRank() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1", new ScoreMemberPair(2, "zsvalue1"),
						new ScoreMemberPair(1, "zsvalue2"),
						new ScoreMemberPair(4, "zsvalue3"),
						new ScoreMemberPair(7, "zsvalue4"),
						new ScoreMemberPair(8, "zsvalue5"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRank(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(1, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", "zsvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				Thread.sleep(CMD_PAUSE_TIME);

				client.zRank(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(0, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", "zsvalue2");

				Thread.sleep(CMD_PAUSE_TIME);

				client.zRank(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(2, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", "zsvalue3");

				Thread.sleep(CMD_PAUSE_TIME);

				client.zRank(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(3, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", "zsvalue4");

				Thread.sleep(CMD_PAUSE_TIME);

				client.zRank(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						// assertEquals(-1, result.longValue());
						System.out.println(result);
						assertTrue(result < 0);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "zskey1", "zsvalue6");
			}
		});
	}

	@Test
	public void testZRem() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1", new ScoreMemberPair(2, "zsvalue1"),
						new ScoreMemberPair(1, "zsvalue2"),
						new ScoreMemberPair(4, "zsvalue3"),
						new ScoreMemberPair(7, "zsvalue4"),
						new ScoreMemberPair(8, "zsvalue5"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRem(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(2, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", "zsvalue2", "zsvalue1", "zsvalue6");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRange(new ResponseCallback<String[]>() {

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
				}, "zskey1", 0, -1);
			}
		});
	}

	@Test
	public void testZRemRangeByRank() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1", new ScoreMemberPair(2, "zsvalue1"),
						new ScoreMemberPair(1, "zsvalue2"),
						new ScoreMemberPair(4, "zsvalue3"),
						new ScoreMemberPair(7, "zsvalue4"),
						new ScoreMemberPair(8, "zsvalue5"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRemRangeByRank(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(3, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", 1, 3);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRange(new ResponseCallback<String[]>() {

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
				}, "zskey1", 0, -1);
			}
		});
	}

	@Test
	public void testZRemRangeByScore() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1", new ScoreMemberPair(2, "zsvalue1"),
						new ScoreMemberPair(1, "zsvalue2"),
						new ScoreMemberPair(4, "zsvalue3"),
						new ScoreMemberPair(7, "zsvalue4"),
						new ScoreMemberPair(8, "zsvalue5"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRemRangeByScore(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(3, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", 4, 10);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRange(new ResponseCallback<String[]>() {

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
				}, "zskey1", 0, -1);
			}
		});
	}

	@Test
	public void testZRevRange() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1", 2, "zsvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1", new ScoreMemberPair(1, "zsvalue2"),
						new ScoreMemberPair(4, "zsvalue3"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRevRange(new ResponseCallback<String[]>() {

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
				}, "zskey1", 0, 1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRevRangeWithScores(
						new ResponseCallback<ScoreMemberPair[]>() {

							@Override
							public void done(ScoreMemberPair[] result) {
								for (ScoreMemberPair p : result) {
									System.out.println(p);
								}
								controller.countDown();
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
								controller.countDown();
							}
						}, "zskey1", 0, -1);

			}
		});
	}

	@Test
	public void testZRevRangeByScore() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1", new ScoreMemberPair(2, "zsvalue1"),
						new ScoreMemberPair(1, "zsvalue2"),
						new ScoreMemberPair(4, "zsvalue3"),
						new ScoreMemberPair(7, "zsvalue4"),
						new ScoreMemberPair(8, "zsvalue5"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRevRangeByScore(new ResponseCallback<String[]>() {

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
				}, "zskey1", 8, 4);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRevRangeByScore(new ResponseCallback<String[]>() {

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
				}, "zskey1", 8, 4, 2, 1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRevRangeByScoreWithScores(
						new ResponseCallback<ScoreMemberPair[]>() {

							@Override
							public void done(ScoreMemberPair[] result) {
								for (ScoreMemberPair p : result) {
									System.out.println(p);
								}
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
							}
						}, "zskey1", 8, 4);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRevRangeByScoreWithScores(
						new ResponseCallback<ScoreMemberPair[]>() {

							@Override
							public void done(ScoreMemberPair[] result) {
								for (ScoreMemberPair p : result) {
									System.out.println(p);
								}
								controller.countDown();
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
								controller.countDown();
							}
						}, "zskey1", 8, 4, 2, 1);
			}
		});
	}

	@Test
	public void testZRevRank() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1", new ScoreMemberPair(2, "zsvalue1"),
						new ScoreMemberPair(1, "zsvalue2"),
						new ScoreMemberPair(4, "zsvalue3"),
						new ScoreMemberPair(7, "zsvalue4"),
						new ScoreMemberPair(8, "zsvalue5"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRevRank(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(3, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", "zsvalue1");
				Thread.sleep(CMD_PAUSE_TIME);

				Thread.sleep(CMD_PAUSE_TIME);

				client.zRevRank(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(4, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", "zsvalue2");

				Thread.sleep(CMD_PAUSE_TIME);

				client.zRevRank(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(2, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", "zsvalue3");

				Thread.sleep(CMD_PAUSE_TIME);

				client.zRevRank(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(1, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey1", "zsvalue4");

				Thread.sleep(CMD_PAUSE_TIME);

				client.zRevRank(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						// assertEquals(-1, result.longValue());
						System.out.println(result);
						assertTrue(result < 0);
						controller.countDown();
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}
				}, "zskey1", "zsvalue6");
			}
		});
	}

	@Test
	public void testZUnionStore() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1",
						new ScoreMemberPair(52, "zsvalue1"),
						new ScoreMemberPair(15, "zsvalue2"),
						new ScoreMemberPair(34, "zsvalue3"),
						new ScoreMemberPair(74, "zsvalue4"),
						new ScoreMemberPair(18, "zsvalue5"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey2",
						new ScoreMemberPair(32, "zsvalue21"),
						new ScoreMemberPair(12, "zsvalue22"),
						new ScoreMemberPair(14, "zsvalue3"),
						new ScoreMemberPair(27, "zsvalue24"),
						new ScoreMemberPair(28, "zsvalue25"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zUnionStore(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(9, result.longValue());

					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey3", "zskey1", "zskey2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRangeWithScores(
						new ResponseCallback<ScoreMemberPair[]>() {

							@Override
							public void done(ScoreMemberPair[] result) {
								assertEquals(9, result.length);
								for (ScoreMemberPair r : result) {
									System.out.println(r);
								}
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
							}
						}, "zskey3", 0, -1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zUnionStore(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(9, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey4", new SortedSetParams().weights(1, 3), "zskey1", "zskey2");

				Thread.sleep(CMD_PAUSE_TIME);

				client.zRangeWithScores(
						new ResponseCallback<ScoreMemberPair[]>() {

							@Override
							public void done(ScoreMemberPair[] result) {
								assertEquals(9, result.length);
								for (ScoreMemberPair r : result) {
									System.out.println(r);
								}
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
							}
						}, "zskey4", 0, -1);

				Thread.sleep(CMD_PAUSE_TIME);

				client.zUnionStore(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(9, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey5", new SortedSetParams().weights(1, 3).aggregate(Aggregate.MAX),
						"zskey1",
						"zskey2");

				Thread.sleep(CMD_PAUSE_TIME);

				client.zRangeWithScores(
						new ResponseCallback<ScoreMemberPair[]>() {

							@Override
							public void done(ScoreMemberPair[] result) {
								assertEquals(9, result.length);
								for (ScoreMemberPair r : result) {
									System.out.println(r);
								}
								controller.countDown();
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
								controller.countDown();
							}
						}, "zskey5", 0, -1);
			}
		});
	}

	@Test
	public void testZInterStore() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey1",
						new ScoreMemberPair(52, "zsvalue1"),
						new ScoreMemberPair(15, "zsvalue2"),
						new ScoreMemberPair(34, "zsvalue3"),
						new ScoreMemberPair(74, "zsvalue4"),
						new ScoreMemberPair(18, "zsvalue5"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "zskey2",
						new ScoreMemberPair(32, "zsvalue21"),
						new ScoreMemberPair(12, "zsvalue2"),
						new ScoreMemberPair(14, "zsvalue3"),
						new ScoreMemberPair(27, "zsvalue24"),
						new ScoreMemberPair(28, "zsvalue25"));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zInterStore(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(2, result.longValue());

					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey3", "zskey1", "zskey2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zRangeWithScores(
						new ResponseCallback<ScoreMemberPair[]>() {

							@Override
							public void done(ScoreMemberPair[] result) {
								assertEquals(2, result.length);
								for (ScoreMemberPair r : result) {
									System.out.println(r);
								}
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
							}
						}, "zskey3", 0, -1);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zInterStore(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(2, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey4", new SortedSetParams().weights(1, 3), "zskey1", "zskey2");

				Thread.sleep(CMD_PAUSE_TIME);

				client.zRangeWithScores(
						new ResponseCallback<ScoreMemberPair[]>() {

							@Override
							public void done(ScoreMemberPair[] result) {
								assertEquals(2, result.length);
								for (ScoreMemberPair r : result) {
									System.out.println(r);
								}
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
							}
						}, "zskey4", 0, -1);

				Thread.sleep(CMD_PAUSE_TIME);

				client.zInterStore(new ResponseCallback<Long>() {

					@Override
					public void done(Long result) {
						assertEquals(2, result.longValue());
					}

					@Override
					public void failed(Throwable cause) {
						fail(cause);
					}
				}, "zskey5", new SortedSetParams().weights(1, 3).aggregate(Aggregate.MIN),
						"zskey1",
						"zskey2");

				Thread.sleep(CMD_PAUSE_TIME);

				client.zRangeWithScores(
						new ResponseCallback<ScoreMemberPair[]>() {

							@Override
							public void done(ScoreMemberPair[] result) {
								assertEquals(2, result.length);
								for (ScoreMemberPair r : result) {
									System.out.println(r);
								}
								controller.countDown();
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
								controller.countDown();
							}
						}, "zskey5", 0, -1);
			}
		});
	}

	@Test
	public void testZScan() {
		doCmdTest(new TestAction() {

			String cursor = "0";

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.flushAll(null);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 10, "value1");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 2, "value2");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 3, "value3");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 4, "value4");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 15, "value5");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 6, "value6");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 7, "value7");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 8, "value8");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 91, "value9");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 10, "value10");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 11, "value11");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 12, "value12");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 13, "value13");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 14, "value14");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 15, "value15");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 16, "value16");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 17, "value17");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 18, "value18");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 19, "value19");
				Thread.sleep(CMD_PAUSE_TIME);

				client.zAdd(null, "key1", 20, "value20");
				Thread.sleep(CMD_PAUSE_TIME);

				final int count = 5;
				client.zScan(
						new ResponseCallback<ScanResult<ScoreMemberPair>>() {

							@Override
							public void done(ScanResult<ScoreMemberPair> result) {
								cursor = result.getCursor();
								System.out.println("cursor:" + cursor);
								System.out.println("result.size:"
										+ result.getResult().size());
								for (ScoreMemberPair r : result.getResult()) {
									System.out.println(r);
								}
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
							}
						}, "key1", cursor);
				Thread.sleep(CMD_PAUSE_TIME);

				client.zScan(
						new ResponseCallback<ScanResult<ScoreMemberPair>>() {

							@Override
							public void done(ScanResult<ScoreMemberPair> result) {
								cursor = result.getCursor();
								System.out.println("cursor:" + cursor);
								System.out.println("result.size:"
										+ result.getResult().size());
								for (ScoreMemberPair r : result.getResult()) {
									System.out.println(r);
								}
							}

							@Override
							public void failed(Throwable cause) {
								fail(cause);
							}
						}, "key1", cursor, new ScanParams().count(count));
				Thread.sleep(CMD_PAUSE_TIME);

				client.zScan(
						new ResponseCallback<ScanResult<ScoreMemberPair>>() {

							@Override
							public void done(ScanResult<ScoreMemberPair> result) {
								cursor = result.getCursor();
								System.out.println("cursor:" + cursor);
								System.out.println("result.size:"
										+ result.getResult().size());
								for (ScoreMemberPair r : result.getResult()) {
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
