package org.handwerkszeug.riak.op;

import java.util.List;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.Bucket;

/**
 * @author taichi
 */
public interface BucketOperations {

	RiakFuture listBuckets(RiakResponseHandler<List<String>> handler);

	RiakFuture listKeys(String bucket, KeyHandler handler);

	RiakFuture getBucket(String bucket, RiakResponseHandler<Bucket> handler);

	RiakFuture setBucket(Bucket bucket, RiakResponseHandler<_> handler);
}
