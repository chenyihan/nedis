package org.cyy.fw.nedis.util;

public class NedisException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 504345938088513512L;

	public NedisException() {
		super();
	}

	public NedisException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public NedisException(String message, Throwable cause) {
		super(message, cause);
	}

	public NedisException(String message) {
		super(message);
	}

	public NedisException(Throwable cause) {
		super(cause);
	}

}
