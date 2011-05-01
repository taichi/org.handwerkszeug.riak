package org.handwerkszeug.riak.op;

import java.util.List;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.KeyResponse;
import org.handwerkszeug.riak.model.RiakFuture;

/**
 * @author taichi
 */
public interface BucketOperations {

	/**
	 * This call can be expensive for the server – do not use in performance
	 * sensitive code.
	 * 
	 * @param handler
	 * @return
	 */
	RiakFuture listBuckets(RiakResponseHandler<List<String>> handler);

	RiakFuture listKeys(String bucket, RiakResponseHandler<KeyResponse> handler);

	RiakFuture getBucket(String bucket, RiakResponseHandler<Bucket> handler);

	RiakFuture setBucket(Bucket bucket, RiakResponseHandler<_> handler);
}
