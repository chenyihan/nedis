package org.cyy.fw.nedis;

import java.util.ArrayList;
import java.util.List;

public class SortedSetParams {

	public enum Aggregate {
		SUM, MIN, MAX;

		public final String text;

		Aggregate() {
			text = name();
		}
	}

	private List<String> params = new ArrayList<String>();

	public SortedSetParams weights(final double... weights) {
		params.add(RedisKeyword.WEIGHTS.getText());
		for (final double weight : weights) {
			params.add(String.valueOf(weight));
		}

		return this;
	}

	public String[] getParams() {
		return params.toArray(new String[0]);
	}

	public SortedSetParams aggregate(final Aggregate aggregate) {
		params.add(RedisKeyword.AGGREGATE.getText());
		params.add(aggregate.text);
		return this;
	}

}
