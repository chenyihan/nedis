package org.cyy.fw.nedis.test.cmd;

import static org.junit.Assert.*;

import org.cyy.fw.nedis.ResponseCallback;
import org.cyy.fw.nedis.util.NedisException;
import org.junit.Test;

public class ConnectionCmdTest extends BaseCmdTest {

	@Test
	public void testAuth() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.auth(new ResponseCallback<String>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}

					@Override
					public void done(String result) {
						assertEquals("OK", result);
						controller.countDown();
					}
				}, "123456");
			}
		});
	}

	@Test
	public void testEcho() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				final String message = "Hello world.";
				client.echo(new ResponseCallback<String>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}

					@Override
					public void done(String result) {
						assertEquals(message, result);
						controller.countDown();
					}
				}, message);
			}
		});
	}

	@Test
	public void testPing() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.ping(new ResponseCallback<String>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}

					@Override
					public void done(String result) {
						assertEquals("PONG", result);
						controller.countDown();
					}
				});
			}
		});
	}

	@Test
	public void testQuit() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {
				client.quit(new ResponseCallback<String>() {

					@Override
					public void failed(Throwable cause) {
						fail(cause);
						controller.countDown();
					}

					@Override
					public void done(String result) {
						assertEquals("OK", result);
						controller.countDown();
					}
				});
			}
		});
	}
}
