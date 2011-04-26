package org.handwerkszeug.riak;

import org.handwerkszeug.riak.op.BucketOperations;
import org.handwerkszeug.riak.op.ObjectKeyOperations;
import org.handwerkszeug.riak.op.Querying;

/**
 * @author taichi
 * @param <R>
 * @param <OP>
 */
public interface RiakAction<R, OP extends BucketOperations & ObjectKeyOperations & Querying> {

	R execute(OP operations);
}
