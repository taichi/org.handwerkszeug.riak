package org.handwerkszeug.riak.mapreduce;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.JsonAppender;
import org.handwerkszeug.riak.nls.Messages;

/**
 * @author taichi
 */
public class DefaultMapReduceQuery implements MapReduceQuery {

	protected MapReduceInputs inputs;

	protected List<MapReducePhase> queries = Collections.emptyList();

	protected long timeout;

	public DefaultMapReduceQuery() {
	}

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

	public ObjectNode prepare() {
		// TODO use streaming API ? JsonGenerator....
		ObjectMapper om = new ObjectMapper();
		ObjectNode root = om.createObjectNode();
		if (this.inputs == null) {
			throw new IllegalStateException(Messages.InputsMustSet);
		} else {
			this.inputs.appendTo(root);
		}

		ArrayNode query = root.putArray(FIELD_QUERY);
		if (this.queries.isEmpty()) {
			throw new IllegalStateException(Messages.QueriesMustSet);
		} else {
			add(query, this.queries);
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
