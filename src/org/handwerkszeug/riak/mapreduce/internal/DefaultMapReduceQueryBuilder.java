package org.handwerkszeug.riak.mapreduce.internal;

import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.location;
import static org.handwerkszeug.riak.util.Validation.notNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.codehaus.jackson.JsonGenerator;
import org.handwerkszeug.riak.mapreduce.MapReduceInput;
import org.handwerkszeug.riak.mapreduce.MapReduceQueryBuilder;
import org.handwerkszeug.riak.mapreduce.grammar.ExecutablePhase;
import org.handwerkszeug.riak.mapreduce.grammar.KeyFilterOrPhase;
import org.handwerkszeug.riak.mapreduce.grammar.PhaseProducer;
import org.handwerkszeug.riak.model.Erlang;
import org.handwerkszeug.riak.util.JsonAppender;

/**
 * @author taichi
 */
public class DefaultMapReduceQueryBuilder<T> implements
		MapReduceQueryBuilder<T> {

	protected MapReduceQueryContext<T> context;
	protected ExecutablePhase<T> executablePhase;
	protected KeyFilterOrPhase<T> keyFilterOrPhase;

	public DefaultMapReduceQueryBuilder(MapReduceQueryContext<T> context) {
		notNull(context, "context");
		this.context = context;
	}

	public void initialize() {
		this.executablePhase = new ExecutablePhaseImpl<T>(this.context);
		this.keyFilterOrPhase = new KeyFilterOrPhaseImpl<T>(this.context,
				this.executablePhase);
	}

	@Override
	public KeyFilterOrPhase<T> inputs(final String bucket) {
		notNull(bucket, "bucket");
		this.context.add(new BucketInput(bucket));
		return this.keyFilterOrPhase;
	}

	@Override
	public PhaseProducer<T> inputs(String bucket, String key) {
		this.context.add(location(bucket, key));
		return this.executablePhase;
	}

	@Override
	public PhaseProducer<T> inputs(String bucket, String key, Object keyData) {
		this.context.add(location(bucket, key, keyData));
		return this.executablePhase;
	}

	@Override
	public PhaseProducer<T> inputs(String bucket, String key, String keyData) {
		this.context.add(location(bucket, key, keyData));
		return this.executablePhase;
	}

	@Override
	public PhaseProducer<T> inputs(String bucket, String key, int keyData) {
		this.context.add(location(bucket, key, keyData));
		return this.executablePhase;
	}

	@Override
	public PhaseProducer<T> inputs(String bucket, String key, long keyData) {
		this.context.add(location(bucket, key, keyData));
		return this.executablePhase;
	}

	@Override
	public PhaseProducer<T> inputs(String bucket, String key, double keyData) {
		this.context.add(location(bucket, key, keyData));
		return this.executablePhase;
	}

	@Override
	public PhaseProducer<T> inputs(String bucket, String key, float keyData) {
		this.context.add(location(bucket, key, keyData));
		return this.executablePhase;
	}

	@Override
	public PhaseProducer<T> inputs(String bucket, String key, BigInteger keyData) {
		this.context.add(location(bucket, key, keyData));
		return this.executablePhase;
	}

	@Override
	public PhaseProducer<T> inputs(String bucket, String key, BigDecimal keyData) {
		this.context.add(location(bucket, key, keyData));
		return this.executablePhase;
	}

	@Override
	public PhaseProducer<T> search(final String index, final String query) {
		this.context.add(new MapReduceInput() {
			@Override
			public void appendTo(JsonGenerator generator) throws IOException {
				generator.writeStartObject();
				JsonAppender arg = ArgAppender.arg(Erlang.riakSearch, index,
						query);
				arg.appendTo(generator);
				generator.writeEndObject();
			}
		});
		return this.executablePhase;
	}

	@Override
	public PhaseProducer<T> inputs(MapReduceInput primary,
			MapReduceInput... inputs) {
		this.context.add(primary, inputs);
		return this.executablePhase;
	}
}
