package org.handwerkszeug.riak;

/**
 * @author taichi
 */
public class RiakException extends RuntimeException {

	private static final long serialVersionUID = -8278289919738623341L;

	public RiakException() {
	}

	public RiakException(Throwable t) {
		super(t);
	}

	public RiakException(String message) {
		super(message);
	}

	public RiakException(String message, Throwable cause) {
		super(message, cause);
	}
}
