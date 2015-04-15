package org.cyy.fw.nedis.test.connection;

import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;

import org.cyy.fw.nedis.NedisClient;
import org.cyy.fw.nedis.NedisClientBuilder;
import org.cyy.fw.nedis.ResponseCallback;
import org.junit.Test;

public class ConnectionPoolTest {

	@Test
	public void testPoolSize1() {
		String host = "192.168.1.107";
		int port = 6379;
		int connectionPoolSize = 5;

		final NedisClient client = new NedisClientBuilder().setServerHost(host)
				.setPort(port).setConnectTimeoutMills(5000)
				.setConnectionPoolSize(connectionPoolSize).build();
		try {
			final CountDownLatch latch = new CountDownLatch(1);
			client.get(new ResponseCallback<String>() {

				@Override
				public void failed(Throwable cause) {
				}

				@Override
				public void done(String result) {
					latch.countDown();

				}
			}, "key1");
			latch.await();
			Thread.sleep(2000);
			assertEquals(1, client.getIdleConnections());

		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			client.shutdown();
		}
	}

	@Test
	public void testPoolSize2() {
		String host = "192.168.1.107";
		int port = 6379;
		int connectionPoolSize = 5;
		final NedisClient client = new NedisClientBuilder().setServerHost(host)
				.setPort(port).setConnectTimeoutMills(5000)
				.setConnectionPoolSize(connectionPoolSize).build();
		try {
			final CountDownLatch latch = new CountDownLatch(1);
			client.get(null, "key1");
			client.get(null, "key2");
			client.get(new ResponseCallback<String>() {

				@Override
				public void failed(Throwable cause) {
				}

				@Override
				public void done(String result) {
					latch.countDown();

				}
			}, "key3");
			latch.await();
			Thread.sleep(5000);
			assertEquals(3, client.getIdleConnections());

		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			client.shutdown();
		}
	}

	@Test
	public void testPoolSize3() {
		String host = "192.168.1.107";
		int port = 6379;
		int connectionPoolSize = 5;

		final NedisClient client = new NedisClientBuilder().setServerHost(host)
				.setPort(port).setConnectTimeoutMills(5000)
				.setConnectionPoolSize(connectionPoolSize).build();
		try {
			final CountDownLatch latch = new CountDownLatch(1);
			client.get(null, "key1");
			client.get(null, "key2");
			client.get(null, "key3");
			client.get(null, "key4");
			client.get(new ResponseCallback<String>() {

				@Override
				public void failed(Throwable cause) {
				}

				@Override
				public void done(String result) {
					latch.countDown();
				}
			}, "key5");
			latch.await();
			Thread.sleep(5000);
			assertEquals(5, client.getIdleConnections());

		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			client.shutdown();
		}
	}

	@Test
	public void testPoolSize4() {
		String host = "192.168.1.107";
		int port = 6379;
		int connectionPoolSize = 5;

		final NedisClient client = new NedisClientBuilder().setServerHost(host)
				.setPort(port).setConnectTimeoutMills(5000)
				.setConnectionPoolSize(connectionPoolSize).build();
		try {
			final CountDownLatch latch = new CountDownLatch(1);
			client.get(null, "key1");
			client.get(null, "key2");
			client.get(null, "key3");
			client.get(null, "key4");
			client.get(null, "key5");
			client.get(new ResponseCallback<String>() {

				@Override
				public void failed(Throwable cause) {
				}

				@Override
				public void done(String result) {
					latch.countDown();

				}
			}, "key6");
			latch.await();
			Thread.sleep(5000);
			assertEquals(5, client.getIdleConnections());

		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			client.shutdown();
		}
	}

	@Test
	public void testPoolSize5() {
		String host = "192.168.1.107";
		int port = 6379;
		int connectionPoolSize = 5;
		int maxConnectionIdleTimeInMills = 5000;

		final NedisClient client = new NedisClientBuilder().setServerHost(host)
				.setPort(port).setConnectTimeoutMills(5000)
				.setConnectionPoolSize(connectionPoolSize)
				.setMaxConnectionIdleTimeInMills(maxConnectionIdleTimeInMills)
				.build();
		try {
			final CountDownLatch latch = new CountDownLatch(1);
			client.get(null, "key1");
			client.get(null, "key2");
			client.get(null, "key3");
			client.get(null, "key4");
			client.get(null, "key5");
			client.get(new ResponseCallback<String>() {

				@Override
				public void failed(Throwable cause) {
				}

				@Override
				public void done(String result) {
					latch.countDown();

				}
			}, "key6");
			latch.await();

			Thread.sleep(maxConnectionIdleTimeInMills + 1000);
			assertEquals(0, client.getIdleConnections());

		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			client.shutdown();
		}
	}

	@Test
	public void testPoolSize6() {
		String host = "192.168.1.107";
		int port = 6379;
		int connectionPoolSize = 5;
		int minIdle = 2;
		int maxConnectionIdleTimeInMills = 5000;

		final NedisClient client = new NedisClientBuilder().setServerHost(host)
				.setPort(port).setConnectTimeoutMills(5000)
				.setConnectionPoolSize(connectionPoolSize)
				.setMaxConnectionIdleTimeInMills(maxConnectionIdleTimeInMills)
				.setMinIdleConnections(minIdle).build();
		try {
			final CountDownLatch latch = new CountDownLatch(1);
			client.get(null, "key1");
			client.get(null, "key2");
			client.get(null, "key3");
			client.get(null, "key4");
			client.get(null, "key5");
			client.get(new ResponseCallback<String>() {

				@Override
				public void failed(Throwable cause) {
				}

				@Override
				public void done(String result) {
					latch.countDown();

				}
			}, "key6");
			latch.await();

			Thread.sleep(maxConnectionIdleTimeInMills + 20000);
			assertEquals(minIdle, client.getIdleConnections());

		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			client.shutdown();
		}
	}

	@Test
	public void testPoolSize7() {
		String host = "192.168.1.107";
		int port = 6379;
		int connectionPoolSize = 5;

		final NedisClient client = new NedisClientBuilder().setServerHost(host)
				.setPort(port).setConnectTimeoutMills(5000)
				.setConnectionPoolSize(connectionPoolSize).build();
		try {
			final CountDownLatch latch = new CountDownLatch(1);
			client.get(null, "key1");
			client.get(null, "key2");
			client.get(null, "key3");
			client.get(null, "key4");
			client.get(null, "key5");
			client.get(new ResponseCallback<String>() {

				@Override
				public void failed(Throwable cause) {
				}

				@Override
				public void done(String result) {
					latch.countDown();

				}
			}, "key6");
			latch.await();
			Thread.sleep(5000);
			client.shutdown();

			final CountDownLatch latch1 = new CountDownLatch(1);
			client.initialize();
			
			client.get(null, "key2");
			client.get(null, "key3");
			client.get(null, "key4");
			client.get(null, "key5");
			client.get(new ResponseCallback<String>() {

				@Override
				public void failed(Throwable cause) {
				}

				@Override
				public void done(String result) {
					latch1.countDown();

				}
			}, "key6");
			latch1.await();
			Thread.sleep(5000);
			client.shutdown();

		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
		}
	}
}
