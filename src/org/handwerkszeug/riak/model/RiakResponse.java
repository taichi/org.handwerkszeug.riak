package org.handwerkszeug.riak.model;

/**
 * @author taichi
 */
public interface RiakResponse {

	String getMessage();

	int getResponseCode();
}
