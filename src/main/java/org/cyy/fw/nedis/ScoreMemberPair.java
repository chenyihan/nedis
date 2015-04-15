package org.cyy.fw.nedis;

public class ScoreMemberPair {

	private double score;
	private String member;

	public ScoreMemberPair(double score, String member) {
		super();
		this.score = score;
		this.member = member;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public String getMember() {
		return member;
	}

	public void setMember(String member) {
		this.member = member;
	}

	@Override
	public String toString() {
		return "ScoreMemberPair [score=" + score + ", member=" + member + "]";
	}

//	public static String[] toArgs(ScoreMemberPair one, ScoreMemberPair... more) {
//
//		int moreLen = more == null ? 0 : more.length;
//		int argLen = one == null ? moreLen : moreLen + 1;
//		if (argLen <= 0) {
//			throw new IllegalArgumentException("One parameters at least.");
//		}
//		ScoreMemberPair[] all = new ScoreMemberPair[argLen];
//		all[0] = one;
//		for (int i = 1; i < argLen; i++) {
//			all[i] = more[i - 1];
//		}
//		return toArgs(all);
//
//	}
//
//	public static String[] toArgs(ScoreMemberPair... scoreMembers) {
//		if (scoreMembers == null) {
//			return null;
//		}
//		int argsLen = scoreMembers.length << 1;
//		if (argsLen <= 0) {
//			throw new IllegalArgumentException("One parameters at least.");
//		}
//		String[] args = new String[argsLen];
//		for (int i = 0; i < scoreMembers.length; i++) {
//			ScoreMemberPair pair = scoreMembers[i];
//			args[i << 1] = String.valueOf(pair.getScore());
//			args[(i << 1) + 1] = pair.getMember();
//		}
//		return args;
//	}
}
