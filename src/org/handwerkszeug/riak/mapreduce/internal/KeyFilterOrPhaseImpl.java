package org.handwerkszeug.riak.mapreduce.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.handwerkszeug.riak.mapreduce.MapReduceKeyFilter;
import org.handwerkszeug.riak.mapreduce.grammar.ExecutablePhase;
import org.handwerkszeug.riak.mapreduce.grammar.KeyFilterOrPhase;
import org.handwerkszeug.riak.model.Function;

/**
 * @author taichi
 */
public class KeyFilterOrPhaseImpl<T> implements KeyFilterOrPhase<T> {

	protected MapReduceQueryContext<T> context;
	protected ExecutablePhase<T> delegate;

	public KeyFilterOrPhaseImpl(MapReduceQueryContext<T> context,
			ExecutablePhase<T> delegate) {
		this.context = context;
		this.delegate = delegate;
	}

	@Override
	public KeyFilterOrPhase<T> keyFilters(MapReduceKeyFilter primary,
			MapReduceKeyFilter... filters) {
		this.context.add(primary, filters);
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, Object arg) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, String arg) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, int arg) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, long arg) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, double arg) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, float arg) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, BigInteger arg) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, BigDecimal arg) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, keep);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, Object arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, String arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, int arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, long arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, double arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, float arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, BigInteger arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> map(Function phase, BigDecimal arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.map(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, Object arg) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, String arg) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, int arg) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, long arg) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, double arg) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, float arg) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, BigInteger arg) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, BigDecimal arg) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, keep);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, Object arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, String arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, int arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, long arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, double arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, float arg, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, BigInteger arg,
			boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, BigDecimal arg,
			boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.reduce(phase, arg, keep);
	}

	@Override
	public ExecutablePhase<T> link(String bucket) {
		this.context.freezeKeyFilters();
		return this.delegate.link(bucket);
	}

	@Override
	public ExecutablePhase<T> link(String bucket, String tag) {
		this.context.freezeKeyFilters();
		return this.delegate.link(bucket, tag);
	}

	@Override
	public ExecutablePhase<T> link(String bucket, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.link(bucket, keep);
	}

	@Override
	public ExecutablePhase<T> link(String bucket, String tag, boolean keep) {
		this.context.freezeKeyFilters();
		return this.delegate.link(bucket, tag, keep);
	}
}
