package org.handwerkszeug.riak.http.rest;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author taichi
 */
public class JsonBucketHolder {

	@JsonProperty("props")
	public JsonBucket props;
}
