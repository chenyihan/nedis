package org.cyy.fw.nedis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ScanParams {

	private List<String> params = new ArrayList<String>();
	public final static String SCAN_POINTER_START = String.valueOf(0);

	public ScanParams match(final String pattern) {
		params.add(RedisKeyword.MATCH.getText());
		params.add(pattern);
		return this;
	}

	public ScanParams count(final int count) {
		params.add(RedisKeyword.COUNT.getText());
		params.add(String.valueOf(count));
		return this;
	}

	public Collection<String> getParams() {
		return Collections.unmodifiableCollection(params);
	}

	public String[] toParams() {
		return params.toArray(new String[0]);
	}

}
