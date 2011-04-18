package org.handwerkszeug.riak;

import org.handwerkszeug.riak.op.BucketOperations;
import org.handwerkszeug.riak.op.ObjectKeyOperations;
import org.handwerkszeug.riak.op.Querying;

/**
 * @author taichi
 */
public interface RiakClient<OP extends BucketOperations & ObjectKeyOperations & Querying> {

	<T> T execute(RiakAction<T, OP> action);

	void dispose();
}
