package org.cyy.fw.nedis.util;

import java.io.UnsupportedEncodingException;

public class TextEncoder {

	public static final String DEFAULT_CHARSET = "UTF-8";

	public static byte[] encode(String text) {
		return encode(text, DEFAULT_CHARSET);
	}

	public static byte[] encode(String text, String charset) {
		if (text == null) {
			return null;
		}
		try {
			return text.getBytes(charset);
		} catch (UnsupportedEncodingException e) {
			throw new NedisException(e);
		}
	}

	public static String decode(byte[] data) {
		return decode(data, DEFAULT_CHARSET);
	}

	public static String decode(byte[] data, String charset) {
		if (data == null) {
			return null;
		}
		try {
			return new String(data, charset);
		} catch (UnsupportedEncodingException e) {
			throw new NedisException(e);
		}
	}
}
