package org.handwerkszeug.riak.ease.internal;

import org.handwerkszeug.riak.ease.RiakCommand;
import org.handwerkszeug.riak.op.RiakOperations;

/**
 * 
 * @author taichi
 * 
 * @param <T>
 * @param <CMD>
 */
public interface ExecutionDelegate<T, CMD extends RiakCommand<?>> {
	<RO extends RiakOperations> void execute(CMD cmd, RO operations,
			ResultHolder<T> holder);
}