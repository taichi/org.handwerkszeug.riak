package org.handwerkszeug.riak.mapreduce.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.ObjectCodec;
import org.codehaus.jackson.map.ObjectMapper;
import org.handwerkszeug.riak.mapreduce.MapReduceInput;
import org.handwerkszeug.riak.mapreduce.MapReduceKeyFilter;
import org.handwerkszeug.riak.mapreduce.grammar.Timeoutable;
import org.handwerkszeug.riak.mapreduce.internal.MapReducePhase.PhaseType;
import org.handwerkszeug.riak.util.Executable;
import org.handwerkszeug.riak.util.JsonAppender;

/**
 * @author taichi
 * @param <T>
 */
public abstract class MapReduceQueryContext<T> implements Executable<T>,
		Timeoutable<T> {

	protected List<JsonAppender> inputs = new ArrayList<JsonAppender>();

	protected List<MapReduceKeyFilter> keyFilters = new ArrayList<MapReduceKeyFilter>();

	protected List<MapReducePhase> phases = new ArrayList<MapReducePhase>();

	protected Long timeout = null;

	protected ObjectCodec codec;

	public MapReduceQueryContext() {
		this(new ObjectMapper());
	}

	public MapReduceQueryContext(ObjectCodec codec) {
		this.codec = codec;
	}

	public void add(MapReduceInput primary, MapReduceInput... inputs) {
		this.inputs.add(primary);
		for (JsonAppender input : inputs) {
			this.inputs.add(input);
		}
		// freeze inputs.
		this.inputs = Collections.unmodifiableList(this.inputs);
	}

	public void add(MapReduceKeyFilter primary, MapReduceKeyFilter... filters) {
		this.keyFilters.add(primary);
		for (MapReduceKeyFilter filter : filters) {
			this.keyFilters.add(filter);
		}
	}

	public void freezeKeyFilters() {
		this.keyFilters = Collections.unmodifiableList(this.keyFilters);
	}

	public void add(PhaseType type, JsonAppender phase) {
		this.phases.add(new MapReducePhase(type, phase));
	}

	public void add(PhaseType type, boolean keep, JsonAppender phase) {
		this.phases.add(new MapReducePhase(type, keep, phase));
	}

	@Override
	public Executable<T> timeout(long millis) {
		this.timeout = millis;
		return this;
	}

	@Override
	public Executable<T> timeout(long timeout, TimeUnit unit) {
		return timeout(unit.toMillis(timeout));
	}

	protected void prepare(OutputStream stream) {
		try {
			JsonFactory factory = new JsonFactory(this.codec);
			JsonGenerator generator = factory.createJsonGenerator(stream);
			prepare(generator);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	protected void prepare(JsonGenerator generator) throws IOException {
		generator.writeStartObject();
		prepareInputs(generator);

		generator.writeArrayFieldStart("query");
		for (Iterator<MapReducePhase> i = this.phases.iterator(); i.hasNext();) {
			MapReducePhase mrp = i.next();
			// emulate official client
			mrp.mayBeKeep(i.hasNext() == false);
			mrp.appendTo(generator);
		}

		generator.writeEndArray();

		if (this.timeout != null) {
			generator.writeNumberField("timeout", this.timeout);
		}
		generator.writeEndObject();
		generator.flush();
	}

	protected void prepareInputs(JsonGenerator generator) throws IOException,
			JsonGenerationException {
		generator.writeFieldName("inputs");
		int size = this.inputs.size();
		if (size < 1) {
			throw new IllegalStateException();
		} else if (size == 1) {
			JsonAppender bucket = this.inputs.get(0);
			if (0 < this.keyFilters.size()) {
				generator.writeStartObject();
				generator.writeFieldName("bucket");
				bucket.appendTo(generator);
				generator.writeArrayFieldStart("key_filters");
				for (JsonAppender f : this.keyFilters) {
					f.appendTo(generator);
				}
				generator.writeEndArray();
				generator.writeEndObject();
			} else {
				bucket.appendTo(generator);
			}
		} else {
			generator.writeStartArray();
			for (JsonAppender i : this.inputs) {
				i.appendTo(generator);
			}
			generator.writeEndArray();
		}
	}

}
