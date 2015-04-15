package org.cyy.fw.nedis;

import org.cyy.fw.nedis.util.NodeInfo;

public class ServerNode extends NodeInfo {

	private String host;
	private int port;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public String toString() {
		return host + ":" + port;
	}
}
