package org.handwerkszeug.riak.op.mapreduce;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.JsonAppender;
import org.handwerkszeug.riak.util.StringUtil;

public abstract class AbstractMapReduceQuery implements MapReduceQuery {

	protected String bucket;

	protected List<MapReduceInput> inputs = new ArrayList<MapReduceInput>();

	protected MapReduceSearchInput search;

	protected List<MapReducePhase> queries = new ArrayList<MapReducePhase>();

	protected long timeout;

	protected AbstractMapReduceQuery() {
	}

	@Override
	public void setInputs(String bucket) {
		if (this.inputs.isEmpty() == false || this.search != null) {
			throw new IllegalStateException();
		}
		this.bucket = bucket;
	}

	@Override
	public void setInputs(Collection<MapReduceInput> inputs) {
		if (StringUtil.isEmpty(this.bucket) == false || this.search != null) {
			throw new IllegalStateException();
		}
		this.inputs.addAll(inputs);
	}

	@Override
	public void setInputs(MapReduceSearchInput search) {
		if (StringUtil.isEmpty(this.bucket) == false
				|| this.inputs.isEmpty() == false) {
			throw new IllegalStateException();
		}
		this.search = search;
	}

	@Override
	public void setQueries(Collection<MapReducePhase> mapReducePhases) {
		this.queries.addAll(mapReducePhases);
	}

	@Override
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	@Override
	public void clear() {
		this.bucket = null;
		this.inputs.clear();
		this.search = null;
		this.queries.clear();
		this.timeout = 0L;
	}

	static final String FIELD_INPUTS = "inputs";
	static final String FIELD_QUERY = "query";
	static final String FIELD_TIMEOUT = "timeout";

	protected ObjectNode prepare() {
		// TODO use streaming API ? JsonGenerator....
		ObjectMapper om = new ObjectMapper();
		ObjectNode root = om.createObjectNode();
		if (StringUtil.isEmpty(this.bucket) == false) {
			root.put(FIELD_INPUTS, this.bucket);
		} else if (this.search != null) {
			this.search.appendTo(root.putObject(FIELD_INPUTS));
		} else {
			add(root.putArray(FIELD_INPUTS), this.inputs);
		}

		add(root.putArray(FIELD_QUERY), this.queries);

		if (0 < this.timeout) {
			root.put(FIELD_TIMEOUT, this.timeout);
		}
		return root;
	}

	protected void add(ArrayNode node,
			List<? extends JsonAppender<ArrayNode>> list) {
		for (JsonAppender<ArrayNode> ja : list) {
			ja.appendTo(node);
		}
	}
}
