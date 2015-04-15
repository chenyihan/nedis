package org.cyy.fw.nedis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SortingParams {

	private List<String> params = new ArrayList<String>();

	/**
	 * Sort by weight in keys.
	 * <p>
	 * Takes a pattern that is used in order to generate the key names of the
	 * weights used for sorting. Weight key names are obtained substituting the
	 * first occurrence of * with the actual value of the elements on the list.
	 * <p>
	 * The pattern for a normal key/value pair is "keyname*" and for a value in
	 * a hash "keyname*->fieldname".
	 * 
	 * @param pattern
	 * @return the SortingParams Object
	 */
	public SortingParams by(final String pattern) {
		params.add(RedisKeyword.BY.getText());
		params.add(pattern);
		return this;
	}

	/**
	 * No sorting.
	 * <p>
	 * This is useful if you want to retrieve a external key (using
	 * {@link #get(String...) GET}) but you don't want the sorting overhead.
	 * 
	 * @return the SortingParams Object
	 */
	public SortingParams nosort() {
		params.add(RedisKeyword.BY.getText());
		params.add(RedisKeyword.NOSORT.getText());
		return this;
	}

	public String[] toParams() {
		return params.toArray(new String[0]);
	}

	public Collection<String> getParams() {
		return Collections.unmodifiableCollection(params);
	}

	/**
	 * Get the Sorting in Descending Order.
	 * 
	 * @return the sortingParams Object
	 */
	public SortingParams desc() {
		params.add(RedisKeyword.DESC.getText());
		return this;
	}

	/**
	 * Get the Sorting in Ascending Order. This is the default order.
	 * 
	 * @return the SortingParams Object
	 */
	public SortingParams asc() {
		params.add(RedisKeyword.ASC.getText());
		return this;
	}

	/**
	 * Limit the Numbers of returned Elements.
	 * 
	 * @param start
	 *            is zero based
	 * @param count
	 * @return the SortingParams Object
	 */
	public SortingParams limit(final int start, final int count) {
		params.add(RedisKeyword.LIMIT.getText());
		params.add(String.valueOf(start));
		params.add(String.valueOf(count));
		return this;
	}

	/**
	 * Sort lexicographicaly. Note that Redis is utf-8 aware assuming you set
	 * the right value for the LC_COLLATE environment variable.
	 * 
	 * @return the SortingParams Object
	 */
	public SortingParams alpha() {
		params.add(RedisKeyword.ALPHA.getText());
		return this;
	}

	/**
	 * Retrieving external keys from the result of the search.
	 * <p>
	 * Takes a pattern that is used in order to generate the key names of the
	 * result of sorting. The key names are obtained substituting the first
	 * occurrence of * with the actual value of the elements on the list.
	 * <p>
	 * The pattern for a normal key/value pair is "keyname*" and for a value in
	 * a hash "keyname*->fieldname".
	 * <p>
	 * To get the list itself use the char # as pattern.
	 * 
	 * @param patterns
	 * @return the SortingParams Object
	 */
	public SortingParams get(String... patterns) {
		for (final String pattern : patterns) {
			params.add(RedisKeyword.GET.getText());
			params.add(pattern);
		}
		return this;
	}

}
