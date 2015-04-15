package org.cyy.fw.nedis.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MD5 hash algorithm
 * 
 * @author yunyun
 * 
 */
public class MD5Hasher {

	private static ThreadLocal<MessageDigest> MD5 = new ThreadLocal<MessageDigest>() {

		@Override
		protected MessageDigest initialValue() {
			try {
				return MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				throw new IllegalStateException(
						"MD5 algorythm is not supported.");
			}
		}

	};

	public static long hash(String key) {
		return hash(TextEncoder.encode(key));
	}

	public static long hash(byte[] key) {
		MessageDigest md5 = MD5.get();
		md5.reset();
		md5.update(key);
		byte[] bKey = md5.digest();
		long res = ((long) (bKey[3] & 0xFF) << 24)
				| ((long) (bKey[2] & 0xFF) << 16)
				| ((long) (bKey[1] & 0xFF) << 8) | (long) (bKey[0] & 0xFF);
		return res;
	}
}
