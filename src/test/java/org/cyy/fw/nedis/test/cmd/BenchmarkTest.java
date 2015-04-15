package org.cyy.fw.nedis.test.cmd;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.cyy.fw.nedis.ResponseCallback;
import org.cyy.fw.nedis.util.NedisException;
import org.junit.Test;

public class BenchmarkTest extends BaseCmdTest {

	private static final Logger LOGGER = Logger.getLogger(BenchmarkTest.class
			.getSimpleName());

	@Test
	public void testSetAndGet() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				client.flushAll(null);
				int repeats = 3000;
				final AtomicInteger writeFail = new AtomicInteger();
				final AtomicInteger readFail = new AtomicInteger();
				final AtomicInteger readHit = new AtomicInteger();
				final AtomicInteger readMiss = new AtomicInteger();
				ResponseCallback<String> writeRespCallback = new ResponseCallback<String>() {

					@Override
					public void done(String result) {
						if (!"OK".equals(result)) {
							writeFail.incrementAndGet();
						}
					}

					@Override
					public void failed(Throwable cause) {
						writeFail.incrementAndGet();
					}
				};
				long start = System.nanoTime();
				for (int i = 0; i < repeats; i++) {
					String key = "key-" + i;
					final String value = "value-" + i;
					client.set(writeRespCallback, key, value);
					final boolean isLast = i == repeats - 1;
					client.get(new ResponseCallback<String>() {

						@Override
						public void done(String result) {
							if (value.equals(value)) {
								readHit.incrementAndGet();
							} else {
								readMiss.incrementAndGet();
							}
							if (isLast) {
								controller.countDown();
							}
						}

						@Override
						public void failed(Throwable cause) {
							readFail.incrementAndGet();
							if (isLast) {
								controller.countDown();
							}
						}
					}, key);
				}
				long costNano = System.nanoTime() - start;
				LOGGER.log(
						Level.INFO,
						"cost time(ms):"
								+ TimeUnit.NANOSECONDS.toMillis(costNano));
				LOGGER.log(Level.INFO, "write failed:" + writeFail.get());
				LOGGER.log(Level.INFO, "read failed:" + readFail.get());
				LOGGER.log(Level.INFO, "read hit:" + readHit.get());
				LOGGER.log(Level.INFO, "read miss:" + readMiss.get());
			}
		});
	}

}
