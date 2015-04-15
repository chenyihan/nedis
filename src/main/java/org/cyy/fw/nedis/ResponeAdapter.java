package org.cyy.fw.nedis;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.cyy.fw.nedis.util.TextEncoder;

/**
 * Response result adapter, it will adjust the response parsed by
 * {@link RedisProtocol} to the friendly result type.
 * 
 * @author yunyun
 * 
 * @param <S>
 *            The source type
 * @param <T>
 *            The final result type
 */
public interface ResponeAdapter<S, T> {

	void setResponseCallback(ResponseCallback<T> respCallBack);

	void done(S source);

	void failed(Throwable cause);
}

abstract class BaseResponseAdapter<S, T> implements ResponeAdapter<S, T> {

	private Logger LOGGER = Logger.getLogger(BaseResponseAdapter.class
			.getSimpleName());
	private ResponseCallback<T> respCallBack;

	public BaseResponseAdapter(ResponseCallback<T> respCallBack) {
		super();
		this.respCallBack = respCallBack;
	}

	@Override
	public void setResponseCallback(ResponseCallback<T> l) {
		this.respCallBack = l;
	}

	@Override
	public void done(S result) {
		if (this.respCallBack == null) {
			return;
		}
		T adjustedResult = adjust(result);
		LOGGER.log(Level.INFO, "result:" + result);
		this.respCallBack.done(adjustedResult);
	}

	@Override
	public void failed(Throwable cause) {
		if (this.respCallBack == null) {
			return;
		}
		LOGGER.log(Level.WARNING, "result:" + cause.getMessage(), cause);
		this.respCallBack.failed(cause);
	}

	protected abstract T adjust(S source);

}

class BooleanResponseAdapter extends BaseResponseAdapter<Object, Boolean> {
	static final int YES = 1;

	public BooleanResponseAdapter(ResponseCallback<Boolean> respCallBack) {
		super(respCallBack);
	}

	@Override
	public Boolean adjust(Object source) {
		if (source == null) {
			return false;
		}
		return source.toString().equals(String.valueOf(YES));
	}

}

class DoubleResponseAdapter extends BaseResponseAdapter<Object, Double> {

	public DoubleResponseAdapter(ResponseCallback<Double> respCallBack) {
		super(respCallBack);
	}

	@Override
	protected Double adjust(Object source) {
		if (source == null) {
			return Double.MIN_VALUE;
		}
		String result = null;
		if (source instanceof byte[]) {
			result = TextEncoder.decode((byte[]) source);
		} else {
			result = source.toString();
		}
		try {
			return Double.valueOf(result);
		} catch (Exception e) {
			// NOOP
		}
		return Double.MIN_VALUE;
	}

}

class ByteResponseAdapter extends BaseResponseAdapter<Object, byte[]> {

	public ByteResponseAdapter(ResponseCallback<byte[]> respCallBack) {
		super(respCallBack);
	}

	@Override
	protected byte[] adjust(Object source) {
		if (source == null) {
			return new byte[0];
		}
		if (source instanceof byte[]) {
			return (byte[]) source;
		}
		return TextEncoder.encode(source.toString());
	}

}

class StringResponseAdapter extends BaseResponseAdapter<Object, String> {

	public StringResponseAdapter(ResponseCallback<String> respCallBack) {
		super(respCallBack);
	}

	@Override
	protected String adjust(Object source) {
		if (source == null) {
			return RedisProtocol.NULL;
		}
		if (source instanceof byte[]) {
			return TextEncoder.decode((byte[]) source);
		}
		return source.toString();
	}

}

class LongResponseAdapter extends BaseResponseAdapter<Object, Long> {

	public LongResponseAdapter(ResponseCallback<Long> respCallBack) {
		super(respCallBack);
	}

	@Override
	protected Long adjust(Object source) {
		if (source instanceof Long) {
			return (Long) source;
		}
		try {
			if (source instanceof byte[]) {
				return Long.valueOf(TextEncoder.decode((byte[]) source));
			}
			return Long.valueOf(source.toString());
		} catch (Exception e) {
			return Long.MIN_VALUE;
		}
	}

}

class StringArrayResponseAdapter extends BaseResponseAdapter<Object, String[]> {

	public StringArrayResponseAdapter(ResponseCallback<String[]> respCallBack) {
		super(respCallBack);
	}

	@Override
	public String[] adjust(Object source) {
		if (source == null) {
			return new String[] { RedisProtocol.NULL };
		}
		Collection<?> list = null;
		if (source.getClass().isArray() && !(source instanceof byte[])) {
			list = Arrays.asList((Object[]) source);
		} else if (Collection.class.isInstance(source)) {
			list = (Collection<?>) source;
		}
		if (list != null) {
			String[] result = new String[list.size()];
			int i = 0;
			for (Object s : list) {
				if (s == null) {
					result[i] = RedisProtocol.NULL;
				} else {
					if (s instanceof byte[]) {
						result[i] = TextEncoder.decode((byte[]) s);
					} else {
						result[i] = s.toString();
					}
				}
				i++;
			}
			return result;
		}
		if (source instanceof byte[]) {
			return new String[] { TextEncoder.decode((byte[]) source) };
		}
		return new String[] { source.toString() };
	}

}

class StringScanResultResponseAdapter extends
		BaseResponseAdapter<List<Object>, ScanResult<String>> {

	public StringScanResultResponseAdapter(
			ResponseCallback<ScanResult<String>> respCallBack) {
		super(respCallBack);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected ScanResult<String> adjust(List<Object> source) {
		Object newcursor = source.get(0);
		String c;
		if (newcursor instanceof byte[]) {
			c = TextEncoder.decode((byte[]) newcursor);
		} else {
			c = newcursor.toString();
		}
		List<String> results = new ArrayList<String>();
		List<Object> rawResults = (List<Object>) source.get(1);
		for (Object o : rawResults) {
			if (o == null) {
				continue;
			}
			if (o instanceof byte[]) {
				results.add(TextEncoder.decode((byte[]) o));
			} else {
				results.add(o.toString());
			}
		}
		return new ScanResult<>(c, results);
	}

}

class HashMapResponseAdapter extends
		BaseResponseAdapter<List<Object>, Map<String, String>> {

	public HashMapResponseAdapter(
			ResponseCallback<Map<String, String>> respCallBack) {
		super(respCallBack);
	}

	@Override
	protected Map<String, String> adjust(List<Object> source) {
		Map<String, String> map = new HashMap<>();
		if (source == null) {
			return map;
		}
		for (int i = 0; i < source.size(); i += 2) {
			Object field = source.get(i);
			Object value = null;
			if (i < source.size() - 1) {
				value = source.get(i + 1);
			}
			String f = null;
			if (field instanceof byte[]) {
				f = TextEncoder.decode((byte[]) field);
			} else {
				f = field.toString();
			}
			String v = null;
			if (value != null && value instanceof byte[]) {
				v = TextEncoder.decode((byte[]) value);
			} else if (value != null) {
				v = value.toString();
			}
			map.put(f, v);
		}
		return map;
	}

}

class ScoreMemberPairResponseAdapter extends
		BaseResponseAdapter<List<Object>, ScoreMemberPair[]> {

	public ScoreMemberPairResponseAdapter(
			ResponseCallback<ScoreMemberPair[]> respCallBack) {
		super(respCallBack);
	}

	@Override
	protected ScoreMemberPair[] adjust(List<Object> source) {
		if (source == null) {
			return new ScoreMemberPair[0];
		}
		ScoreMemberPair[] pairs = new ScoreMemberPair[source.size() >> 1];
		for (int i = 0; i < source.size(); i += 2) {
			Object value = source.get(i);
			Object score = null;
			if (i < source.size() - 1) {
				score = source.get(i + 1);
			}
			String v = null;
			if (value instanceof byte[]) {
				v = TextEncoder.decode((byte[]) value);
			} else {
				v = value.toString();
			}
			String s = null;
			if (score != null && score instanceof byte[]) {
				s = TextEncoder.decode((byte[]) score);
			} else if (score != null) {
				s = score.toString();
			}
			if (i % 2 == 0) {
				pairs[i >> 1] = new ScoreMemberPair(Double.valueOf(s), v);
			}
		}
		return pairs;
	}

}

class HashScanResultResponseAdapter
		extends
		BaseResponseAdapter<List<Object>, ScanResult<Map.Entry<String, String>>> {

	public HashScanResultResponseAdapter(
			ResponseCallback<ScanResult<Entry<String, String>>> respCallBack) {
		super(respCallBack);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected ScanResult<Entry<String, String>> adjust(List<Object> source) {
		List<Map.Entry<String, String>> results = new ArrayList<Map.Entry<String, String>>();
		if (source == null) {
			return new ScanResult<Map.Entry<String, String>>("0", results);
		}
		Object newcursor = source.get(0);
		String c;
		if (newcursor instanceof byte[]) {
			c = TextEncoder.decode((byte[]) newcursor);
		} else {
			c = newcursor.toString();
		}
		List<Object> rawResults = (List<Object>) source.get(1);
		for (int i = 0; i < rawResults.size(); i += 2) {
			Object field = rawResults.get(i);
			Object value = null;
			if (i < rawResults.size() - 1) {
				value = rawResults.get(i + 1);
			}
			String f = null;
			if (field instanceof byte[]) {
				f = TextEncoder.decode((byte[]) field);
			} else {
				f = field.toString();
			}
			String v = null;
			if (value != null && value instanceof byte[]) {
				v = TextEncoder.decode((byte[]) value);
			} else if (value != null) {
				v = value.toString();
			}
			results.add(new AbstractMap.SimpleEntry<String, String>(f, v));
		}
		return new ScanResult<Map.Entry<String, String>>(c, results);
	}

}

class ScoreMemberScanResultResponseAdapter extends
		BaseResponseAdapter<List<Object>, ScanResult<ScoreMemberPair>> {

	public ScoreMemberScanResultResponseAdapter(
			ResponseCallback<ScanResult<ScoreMemberPair>> respCallBack) {
		super(respCallBack);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected ScanResult<ScoreMemberPair> adjust(List<Object> source) {
		List<ScoreMemberPair> results = new ArrayList<ScoreMemberPair>();
		if (source == null) {
			return new ScanResult<ScoreMemberPair>("0", results);
		}
		Object newcursor = source.get(0);
		String c;
		if (newcursor instanceof byte[]) {
			c = TextEncoder.decode((byte[]) newcursor);
		} else {
			c = newcursor.toString();
		}
		List<Object> rawResults = (List<Object>) source.get(1);
		for (int i = 0; i < rawResults.size(); i += 2) {
			Object field = rawResults.get(i);
			Object value = null;
			if (i < rawResults.size() - 1) {
				value = rawResults.get(i + 1);
			}
			String member = null;
			if (field instanceof byte[]) {
				member = TextEncoder.decode((byte[]) field);
			} else {
				member = field.toString();
			}
			String scoreStr = null;
			if (value != null && value instanceof byte[]) {
				scoreStr = TextEncoder.decode((byte[]) value);
			} else if (value != null) {
				scoreStr = value.toString();
			}
			Double score = 0d;
			try {
				score = Double.valueOf(scoreStr);
			} catch (Exception e) {
				// NOOP
			}
			results.add(new ScoreMemberPair(score, member));
		}
		return new ScanResult<ScoreMemberPair>(c, results);
	}

}
