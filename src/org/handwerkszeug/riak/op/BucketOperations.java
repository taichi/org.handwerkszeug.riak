package org.handwerkszeug.riak.op;

import java.util.List;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.Bucket;

/**
 * @author taichi
 */
public interface BucketOperations {

	void listBuckets(RiakResponseHandler<List<String>> handler);

	void listKeys(String bucket, KeyHandler handler);

	void getBucket(String bucket, RiakResponseHandler<Bucket> handler);

	void setBucket(Bucket bucket, RiakResponseHandler<_> handler);
}
