package org.handwerkszeug.riak.transport.rest.internal;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.transport.rest.JsonBucket;

/**
 * @author taichi
 */
public class BucketHolder {

	@JsonSerialize(as = JsonBucket.class)
	@JsonDeserialize(as = JsonBucket.class)
	@JsonProperty("props")
	public Bucket props;
}
