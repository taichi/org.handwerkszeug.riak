package org.handwerkszeug.riak.http.rest;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.handwerkszeug.riak.model.Bucket;

/**
 * @author taichi
 */
public class BucketHolder {

	@JsonSerialize(as = JsonBucket.class)
	@JsonDeserialize(as = JsonBucket.class)
	@JsonProperty("props")
	public Bucket props;
}
