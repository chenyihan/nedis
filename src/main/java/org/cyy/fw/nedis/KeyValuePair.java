package org.cyy.fw.nedis;

public class KeyValuePair {

	private String key;
	private String value;

	public KeyValuePair(String key, String value) {
		super();
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	// public static String[] toArgs(KeyValuePair one, KeyValuePair... more) {
	// int moreLen = more == null ? 0 : more.length;
	// int argLen = one == null ? moreLen : moreLen + 1;
	// if (argLen <= 0) {
	// throw new IllegalArgumentException("One parameters at least.");
	// }
	// KeyValuePair[] all = new KeyValuePair[argLen];
	// all[0] = one;
	// for (int i = 1; i < argLen; i++) {
	// all[i] = more[i - 1];
	// }
	// return toArgs(all);
	// }
	//
	// public static String[] toArgs(KeyValuePair... pairs) {
	// if (pairs == null) {
	// return null;
	// }
	// int argsLen = pairs.length << 1;
	// if (argsLen <= 0) {
	// throw new IllegalArgumentException("One parameters at least.");
	// }
	// String[] args = new String[argsLen];
	// for (int i = 0; i < pairs.length; i++) {
	// KeyValuePair pair = pairs[i];
	// args[i << 1] = pair.getKey();
	// args[(i << 1) + 1] = pair.getValue();
	// }
	// return args;
	// }
}
