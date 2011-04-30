package org.handwerkszeug.riak;

import org.handwerkszeug.riak.op.BucketOperations;
import org.handwerkszeug.riak.op.ObjectKeyOperations;
import org.handwerkszeug.riak.op.Querying;

/**
 * @author taichi
 * @param <OP>
 */
public interface RiakAction<OP extends BucketOperations & ObjectKeyOperations & Querying> {

	void execute(OP operations);
}
