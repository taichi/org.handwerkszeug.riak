package org.handwerkszeug.riak.mapreduce.internal;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.handwerkszeug.riak.util.JsonAppender;

/**
 * @author taichi
 */
public class MapReducePhase implements JsonAppender {

	public enum PhaseType {
		map, reduce, link;
	}

	final PhaseType phaseType;

	Boolean keep;

	final JsonAppender phase;

	public MapReducePhase(PhaseType type, JsonAppender phase) {
		this.phaseType = type;
		this.phase = phase;
	}

	public MapReducePhase(PhaseType type, boolean keep, JsonAppender phase) {
		this(type, phase);
		this.keep = keep;
	}

	public void mayBeKeep(boolean is) {
		this.keep = this.keep == null ? is : this.keep;
	}

	@Override
	public void appendTo(JsonGenerator generator) throws IOException {
		generator.writeStartObject();
		generator.writeFieldName(this.phaseType.name());

		generator.writeStartObject();
		this.phase.appendTo(generator);
		if (this.keep) {
			generator.writeBooleanField("keep", this.keep);
		}
		generator.writeEndObject();

		generator.writeEndObject();
	}

}
