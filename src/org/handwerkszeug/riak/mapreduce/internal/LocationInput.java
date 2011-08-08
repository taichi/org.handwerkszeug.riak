package org.handwerkszeug.riak.mapreduce.internal;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.handwerkszeug.riak.mapreduce.MapReduceInput;

/**
 * @author taichi
 */
public class LocationInput implements MapReduceInput {
	final String bucket;
	final String key;

	public LocationInput(String bucket, String key) {
		this.bucket = bucket;
		this.key = key;
	}

	@Override
	public void appendTo(JsonGenerator generator) throws IOException {
		generator.writeStartArray();
		generator.writeString(this.bucket);
		generator.writeString(this.key);
		appendKeyData(generator);
		generator.writeEndArray();
	}

	protected void appendKeyData(JsonGenerator generator) throws IOException {
	}
}