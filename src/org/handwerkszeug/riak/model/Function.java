package org.handwerkszeug.riak.model;

import java.io.IOException;


import org.codehaus.jackson.JsonGenerator;
import org.handwerkszeug.riak.util.JsonAppender;

public abstract class Function implements JsonAppender {
	final Language language;

	protected Function(Language language) {
		this.language = language;
	}

	@Override
	public void appendTo(JsonGenerator generator) throws IOException {
		generator.writeStringField("language", this.language.name());
		appendBody(generator);
	}

	protected abstract void appendBody(JsonGenerator generator)
			throws IOException;

}
