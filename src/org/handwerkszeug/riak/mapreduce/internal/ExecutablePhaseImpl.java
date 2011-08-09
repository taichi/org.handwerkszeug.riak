package org.handwerkszeug.riak.mapreduce.internal;

import static org.handwerkszeug.riak.mapreduce.internal.ArgAppender.arg;
import static org.handwerkszeug.riak.mapreduce.internal.PhaseType.link;
import static org.handwerkszeug.riak.mapreduce.internal.PhaseType.map;
import static org.handwerkszeug.riak.mapreduce.internal.PhaseType.reduce;
import static org.handwerkszeug.riak.util.Validation.notNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonGenerator;
import org.handwerkszeug.riak.mapreduce.grammar.ExecutablePhase;
import org.handwerkszeug.riak.model.Function;
import org.handwerkszeug.riak.util.Executable;
import org.handwerkszeug.riak.util.JsonAppender;

public class ExecutablePhaseImpl<T> implements ExecutablePhase<T> {

	final MapReduceQueryContext<T> context;

	public ExecutablePhaseImpl(MapReduceQueryContext<T> context) {
		this.context = context;
	}

	@Override
	public ExecutablePhase<T> map(Function phase) {
		this.context.add(map, false, phase);
		return this;
	}

	@Override
	public ExecutablePhase<T> map(final Function phase, final Object arg) {
		this.context.add(map, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, String arg) {
		this.context.add(map, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, int arg) {
		this.context.add(map, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, long arg) {
		this.context.add(map, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, double arg) {
		this.context.add(map, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, float arg) {
		this.context.add(map, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, BigInteger arg) {
		this.context.add(map, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, BigDecimal arg) {
		this.context.add(map, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, boolean keep) {
		this.context.add(map, keep, phase);
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, Object arg, boolean keep) {
		this.context.add(map, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, String arg, boolean keep) {
		this.context.add(map, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, int arg, boolean keep) {
		this.context.add(map, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, long arg, boolean keep) {
		this.context.add(map, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, double arg, boolean keep) {
		this.context.add(map, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, float arg, boolean keep) {
		this.context.add(map, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, BigInteger arg, boolean keep) {
		this.context.add(map, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> map(Function phase, BigDecimal arg, boolean keep) {
		this.context.add(map, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase) {
		this.context.add(reduce, false, phase);
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, Object arg) {
		this.context.add(reduce, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, String arg) {
		this.context.add(reduce, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, int arg) {
		this.context.add(reduce, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, long arg) {
		this.context.add(reduce, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, double arg) {
		this.context.add(reduce, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, float arg) {
		this.context.add(reduce, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, BigInteger arg) {
		this.context.add(reduce, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, BigDecimal arg) {
		this.context.add(reduce, false, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, boolean keep) {
		this.context.add(reduce, keep, phase);
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, Object arg, boolean keep) {
		this.context.add(reduce, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, String arg, boolean keep) {
		this.context.add(reduce, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, int arg, boolean keep) {
		this.context.add(reduce, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, long arg, boolean keep) {
		this.context.add(reduce, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, double arg, boolean keep) {
		this.context.add(reduce, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, float arg, boolean keep) {
		this.context.add(reduce, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, BigInteger arg,
			boolean keep) {
		this.context.add(reduce, keep, arg(phase, arg));
		return this;
	}

	@Override
	public ExecutablePhase<T> reduce(Function phase, BigDecimal arg,
			boolean keep) {
		this.context.add(reduce, keep, arg(phase, arg));
		return this;
	}

	protected void appendBucket(JsonGenerator generator, String bucket)
			throws IOException {
		notNull(bucket, "bucket");
		generator.writeStringField("bucket", bucket);
	}

	protected void appendTag(JsonGenerator generator, String tag)
			throws IOException {
		notNull(tag, "tag");
		generator.writeStringField("tag", tag);
	}

	@Override
	public ExecutablePhase<T> link(final String bucket) {
		this.context.add(link, false, new JsonAppender() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				appendBucket(generator, bucket);
			}
		});
		return this;
	}

	@Override
	public ExecutablePhase<T> link(final String bucket, final String tag) {
		this.context.add(link, false, new JsonAppender() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				appendBucket(generator, bucket);
				appendTag(generator, tag);
			}
		});
		return this;
	}

	@Override
	public ExecutablePhase<T> link(final String bucket, boolean keep) {
		this.context.add(link, keep, new JsonAppender() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				appendBucket(generator, bucket);
			}
		});
		return this;
	}

	@Override
	public ExecutablePhase<T> link(final String bucket, final String tag,
			boolean keep) {
		this.context.add(link, keep, new JsonAppender() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				appendBucket(generator, bucket);
				appendTag(generator, tag);
			}
		});
		return this;
	}

	@Override
	public Executable<T> timeout(long millis) {
		return this.context.timeout(millis);
	}

	@Override
	public Executable<T> timeout(long timeout, TimeUnit unit) {
		return this.context.timeout(timeout, unit);
	}

	@Override
	public T execute() {
		return this.context.execute();
	}

}
