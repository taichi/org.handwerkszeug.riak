package org.handwerkszeug.riak;

import org.handwerkszeug.riak.op.RiakOperations;

public interface RiakAction<R> {

	R execute(RiakOperations operations);
}
