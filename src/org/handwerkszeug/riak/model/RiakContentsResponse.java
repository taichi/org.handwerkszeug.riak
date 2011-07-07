package org.handwerkszeug.riak.model;

/**
 * @author taichi
 */
public class RiakContentsResponse<T> extends AbstractRiakResponse {

	final T contents;

	public RiakContentsResponse(T contents) {
		this.contents = contents;
	}

	public T getContents() {
		return this.contents;
	}
}
