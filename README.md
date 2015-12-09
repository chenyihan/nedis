A redis client, this client makes use of Netty for network communication framework, this client is named Nedis due to Netty+Redis. All of command request of this client are asynchronously, your program needn't to wait for the request to finish, when the request is finished, the framework will invoke the ResponseCallback instead, if the command has been processed by the server successfully, ResponseCallback.done(Object) will be called, otherwise, ResponseCallback.failed(Throwable) will called, The Response Callback is a generic type, each command may generate a difference type, such as SET command will return a string type result, DEL command will return a long integer type result and EXIST command will return a boolean type result so on. 

A client instance can be used to connection one server only, if you have more than one server instances, you have to resort to some client instances more, all of these client instances share one thread pool(EventLoopGroup) and are managed by NedisClientManager, the client have to register itself to the manager, the manager provides a thread pool for all clients, and it will be shutdown after all clients are shutdown. 

This client use a connection pool, the command can reuse the connections to prevent establishing a connection to the server frequently, due to the heavy performance overhead. You can set the pool size by calling setConnectionPoolSize(int), see ConnectionPool for more details of the connection pool. 

You must call the initialize() before send any command by this client, otherwise the client will not work and throws a Exception. The framework suggest you use the NedisClientBuilder.build() to create the client instance, it will call initialize() for you. you can create the client instance follow the sample code: 
```java
 String host = "192.168.1.107";
 int port = 6379;
 client = new NedisClientBuilder().setServerHost(host).setPort(port)
 		.setConnectTimeoutMills(connectTimeoutMills).setConnectionPoolSize(5)
 		.build();
```
 
See NedisClientBuilder for more parameters, and send command by client follow the below sample code: 
 
 ```java
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
 ```
 
 
 
The framework also support key sharding, If your system have several server instance and want to distribute keys to all servers evenly, the ShardedNedis will help you, see ShardedNedis for more details.

The nedis support most redis commands, the other commands will be supported in the next version.

JDK 7 required.
