package org.jboss.netty.handler.codec.http;

/**
 * @author taichi
 */
public class UnknownBoundaryException extends IllegalStateException {

	private static final long serialVersionUID = 2674367951481056805L;

	final String line;

	public UnknownBoundaryException(String line) {
		this.line = line;
	}

	public String getLine() {
		return this.line;
	}
}
