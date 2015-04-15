package org.cyy.fw.nedis;

/**
 * The response callback, the method {@link #done(Object)} will be called when
 * the command request is done successfully, otherwise the
 * {@link #failed(Throwable)} will be called instead.
 * 
 * @author yunyun
 * 
 * @param <T>
 *            The result generic type
 */
public interface ResponseCallback<T> {
	/**
	 * The request has been done successfully.
	 * 
	 * @param result
	 *            the request result, its type is generic
	 */
	void done(T result);

	/**
	 * The request has been failed by some cause.
	 * 
	 * @param cause
	 *            the failed cause
	 */
	void failed(Throwable cause);
}