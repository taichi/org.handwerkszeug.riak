package org.handwerkszeug.riak.mapreduce.internal;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.handwerkszeug.riak.mapreduce.MapReduceInput;

public class BucketInput implements MapReduceInput {
	final String bucket;

	public BucketInput(String bucket) {
		this.bucket = bucket;
	}

	@Override
	public void appendTo(JsonGenerator generator) throws IOException {
		generator.writeString(this.bucket);
	}
}