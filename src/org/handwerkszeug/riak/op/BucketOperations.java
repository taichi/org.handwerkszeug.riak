package org.handwerkszeug.riak.op;

import java.util.List;

import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.model.Bucket;

/**
 * @author taichi
 */
public interface BucketOperations {

	RiakResponse<List<String>> listBuckets();

	RiakResponse<Iterable<String>> listKeys(String bucket);

	RiakResponse<Bucket> getBucket(String bucket);

	RiakResponse<_> setBucket(String bucket, Bucket props);
}
