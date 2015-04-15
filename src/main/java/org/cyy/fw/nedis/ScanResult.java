package org.cyy.fw.nedis;

import java.util.List;

public class ScanResult<T> {
	private String cursor;
	private List<T> results;

	public ScanResult(String cursor, List<T> results) {
		// this(TextEncoder.encode(cursor), results);
		this.cursor = cursor;
		this.results = results;
	}

	// public ScanResult(byte[] cursor, List<T> results) {
	// this.cursor = cursor;
	// this.results = results;
	// }

	public String getCursor() {
		return cursor;
	}

	// public byte[] getCursorAsBytes() {
	// return cursor;
	// }

	public List<T> getResult() {
		return results;
	}
}
