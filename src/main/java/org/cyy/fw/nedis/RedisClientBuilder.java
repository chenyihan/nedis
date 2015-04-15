package org.cyy.fw.nedis;

public interface RedisClientBuilder extends ClientConfig<RedisClientBuilder> {

	RedisClientBuilder setServerHost(String host);

	RedisClientBuilder setPort(int port);

	NedisClient build();
}
