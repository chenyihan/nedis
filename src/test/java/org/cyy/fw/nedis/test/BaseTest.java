package org.cyy.fw.nedis.test;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.cyy.fw.nedis.util.NedisException;

public class BaseTest {
	protected interface TestAction {
		void doTest() throws InterruptedException, NedisException;
	}

	private static final Logger LOGGER = Logger.getLogger(BaseTest.class
			.getSimpleName());
	protected static final int CMD_PAUSE_TIME = 200;
	protected int connectTimeoutMills = 5000;
	protected CountDownLatch controller;

	protected void doCmdTest(TestAction testAction) {
		if (testAction == null) {
			return;
		}
		try {
			controller = new CountDownLatch(1);
			testAction.doTest();

			// blocking until unit test completed
			controller.await();
		} catch (InterruptedException | NedisException e) {
			fail(e);
		}
	}

	protected void fail(Throwable cause) {
		LOGGER.log(Level.WARNING, cause.getMessage(), cause);
	}

}
