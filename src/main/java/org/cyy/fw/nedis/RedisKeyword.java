package org.cyy.fw.nedis;

import org.cyy.fw.nedis.util.TextEncoder;

public enum RedisKeyword {
	AGGREGATE, ALPHA, ASC, BY, DESC, GET, LIMIT, MESSAGE, NO, NOSORT, PMESSAGE, PSUBSCRIBE, PUNSUBSCRIBE, OK, ONE, QUEUED, SET, STORE, SUBSCRIBE, UNSUBSCRIBE, WEIGHTS, WITHSCORES, RESETSTAT, RESET, FLUSH, EXISTS, LOAD, KILL, LEN, REFCOUNT, ENCODING, IDLETIME, AND, OR, XOR, NOT, GETNAME, SETNAME, LIST, MATCH, COUNT, REPLACE;
	public final byte[] binary;
	private String text;

	private RedisKeyword() {
		text = name();
		binary = TextEncoder.encode(this.name().toLowerCase());
	}

	public byte[] getBinary() {
		return binary;
	}

	public String getText() {
		return text;
	}
}