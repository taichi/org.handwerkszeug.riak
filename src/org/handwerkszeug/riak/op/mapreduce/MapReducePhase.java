package org.handwerkszeug.riak.op.mapreduce;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

/**
 * 
 * @author taichi
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_mapred_json.erl">riak_kv_mapred_json.erl</a>
 */
public abstract class MapReducePhase {

	public enum PhaseType {
		map, reduce, link;
	}

	final PhaseType phase;

	final boolean keep;

	public MapReducePhase(PhaseType phase) {
		this(phase, false);
	}

	public MapReducePhase(PhaseType phase, boolean keep) {
		this.phase = phase;
		this.keep = keep;
	}

	public ObjectNode toJson() {
		// TODO use streaming API ? JsonGenerator....
		ObjectMapper om = new ObjectMapper();
		ObjectNode rootNode = om.createObjectNode();
		ObjectNode phaseNode = om.createObjectNode();
		appendPhase(phaseNode);
		phaseNode.put("keep", this.keep);
		rootNode.put(phase.name(), phaseNode);
		return rootNode;
	}

	protected abstract void appendPhase(ObjectNode json);
}
