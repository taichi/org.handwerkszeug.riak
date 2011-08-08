package org.handwerkszeug.riak.mapreduce.internal;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.handwerkszeug.riak.util.JsonAppender;

public class MapReducePhase implements JsonAppender {

	final PhaseType phaseType;

	final boolean keep;

	final JsonAppender phase;

	public MapReducePhase(PhaseType type, boolean keep, JsonAppender phase) {
		this.phaseType = type;
		this.keep = keep;
		this.phase = phase;
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
