package org.cyy.fw.nedis;

public class ShardedException extends Throwable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2071135414223330639L;

	private ServerNode server;

	public ShardedException(Throwable cause, ServerNode server) {
		super(cause);
		this.server = server;
	}

	public ServerNode getServer() {
		return server;
	}

	public void setServer(ServerNode server) {
		this.server = server;
	}

}
