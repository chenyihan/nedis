package org.cyy.fw.nedis;

public final class CmdArgumentTool {
	public static String[] combineArgs(String key, String... value) {
		return combineArgs(new String[] { key }, value);
	}

	public static String[] combineArgs(String[] args1, String... args2) {
		int argsLen1 = args1 == null ? 0 : args1.length;
		int argsLen2 = args2 == null ? 0 : args2.length;
		int argsLen = argsLen1 + argsLen2;
		String[] args = new String[argsLen];
		for (int i = 0; i < argsLen1; i++) {
			args[i] = args1[i];
		}
		for (int i = 0; i < argsLen2; i++) {
			args[i + argsLen1] = args2[i];
		}
		return args;
	}

	public static String[] combineArgs(KeyValuePair one, KeyValuePair... more) {
		int moreLen = more == null ? 0 : more.length;
		int argLen = one == null ? moreLen : moreLen + 1;
		if (argLen <= 0) {
			throw new IllegalArgumentException("One parameters at least.");
		}
		KeyValuePair[] all = new KeyValuePair[argLen];
		all[0] = one;
		for (int i = 1; i < argLen; i++) {
			all[i] = more[i - 1];
		}
		return combineArgs(all);
	}

	public static String[] combineArgs(KeyValuePair... pairs) {
		if (pairs == null) {
			return null;
		}
		int argsLen = pairs.length << 1;
		if (argsLen <= 0) {
			throw new IllegalArgumentException("One parameters at least.");
		}
		String[] args = new String[argsLen];
		for (int i = 0; i < pairs.length; i++) {
			KeyValuePair pair = pairs[i];
			args[i << 1] = pair.getKey();
			args[(i << 1) + 1] = pair.getValue();
		}
		return args;
	}

	public static String[] combineArgs(ScoreMemberPair one,
			ScoreMemberPair... more) {

		int moreLen = more == null ? 0 : more.length;
		int argLen = one == null ? moreLen : moreLen + 1;
		if (argLen <= 0) {
			throw new IllegalArgumentException("One parameters at least.");
		}
		ScoreMemberPair[] all = new ScoreMemberPair[argLen];
		all[0] = one;
		for (int i = 1; i < argLen; i++) {
			all[i] = more[i - 1];
		}
		return combineArgs(all);

	}

	public static String[] combineArgs(ScoreMemberPair... scoreMembers) {
		if (scoreMembers == null) {
			return null;
		}
		int argsLen = scoreMembers.length << 1;
		if (argsLen <= 0) {
			throw new IllegalArgumentException("One parameters at least.");
		}
		String[] args = new String[argsLen];
		for (int i = 0; i < scoreMembers.length; i++) {
			ScoreMemberPair pair = scoreMembers[i];
			args[i << 1] = String.valueOf(pair.getScore());
			args[(i << 1) + 1] = pair.getMember();
		}
		return args;
	}
}
