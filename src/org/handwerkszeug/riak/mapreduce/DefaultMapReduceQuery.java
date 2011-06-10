package org.handwerkszeug.riak.mapreduce;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.util.JsonAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 */
public class DefaultMapReduceQuery implements MapReduceQuery {

	static final Logger LOG = LoggerFactory
			.getLogger(DefaultMapReduceQuery.class);

	protected MapReduceInputs inputs;

	protected List<MapReducePhase> queries = Collections.emptyList();

	protected long timeout;

	public DefaultMapReduceQuery() {
	}

	@Override
	public void setInputs(MapReduceInputs inputs) {
		this.inputs = inputs;
	}

	@Override
	public void setQueries(MapReducePhase... mapReducePhases) {
		this.queries = Arrays.asList(mapReducePhases);
	}

	@Override
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	static final String FIELD_QUERY = "query";
	static final String FIELD_TIMEOUT = "timeout";

	public void prepare(OutputStream out) {
		try {
			// TODO set JsonGenerator to prepare ?
			ObjectMapper om = new ObjectMapper();
			JsonNode node = prepare(om);
			if (LOG.isDebugEnabled()) {
				LOG.debug(Markers.BOUNDARY, node.toString());
			}
			JsonFactory factory = new JsonFactory(om);
			JsonGenerator gen = factory.createJsonGenerator(out,
					JsonEncoding.UTF8);
			gen.writeTree(node);
			gen.close();
		} catch (IOException e) {
			throw new RiakException(e);
		}
	}

	protected ObjectNode prepare(ObjectMapper om) {
		ObjectNode root = om.createObjectNode();
		if (this.inputs == null) {
			throw new IllegalStateException(Messages.InputsMustSet);
		} else {
			this.inputs.appendTo(root);
		}

		if (this.queries.isEmpty()) {
			throw new IllegalStateException(Messages.QueriesMustSet);
		} else {
			add(root.putArray(FIELD_QUERY), this.queries);
		}

		if (0 < this.timeout) {
			root.put(FIELD_TIMEOUT, this.timeout);
		}
		return root;
	}

	protected void add(ArrayNode node,
			Iterable<? extends JsonAppender<ArrayNode>> list) {
		for (JsonAppender<ArrayNode> ja : list) {
			ja.appendTo(node);
		}
	}
}
