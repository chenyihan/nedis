package org.cyy.fw.nedis.test.cmd;

import static org.junit.Assert.assertEquals;

import org.cyy.fw.nedis.ResponseCallback;
import org.cyy.fw.nedis.util.NedisException;
import org.junit.Test;

public class ServerCmdTest extends BaseCmdTest {

	@Test
	public void testFlushDB() {
		doCmdTest(new TestAction() {

			@Override
			public void doTest() throws InterruptedException, NedisException {

				ResponseCallback<String> respCallBack = new ResponseCallback<String>() {

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
				};
				client.flushDB(respCallBack);

			}
		});
	}

}
