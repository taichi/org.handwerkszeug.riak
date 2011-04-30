package org.handwerkszeug.riak;

import org.handwerkszeug.riak.op.BucketOperations;
import org.handwerkszeug.riak.op.ObjectKeyOperations;
import org.handwerkszeug.riak.op.Querying;

/**
 * @author taichi
 */
public interface RiakClient<OP extends BucketOperations & ObjectKeyOperations & Querying> {

	void execute(RiakAction<OP> action);

	void dispose();
}
