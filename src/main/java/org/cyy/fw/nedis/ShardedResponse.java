package org.cyy.fw.nedis;

public class ShardedResponse<T> {

	private T result;
	private ServerNode server;

	public ShardedResponse(T result, ServerNode server) {
		super();
		this.result = result;
		this.server = server;
	}

	public T getResult() {
		return result;
	}

	public void setResult(T result) {
		this.result = result;
	}

	public ServerNode getServer() {
		return server;
	}

	public void setServer(ServerNode server) {
		this.server = server;
	}

	@Override
	public String toString() {
		return "ShardedResponse [result=" + result + ", server=" + server + "]";
	}

}
