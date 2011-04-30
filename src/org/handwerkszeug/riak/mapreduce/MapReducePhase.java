package org.handwerkszeug.riak.mapreduce;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.util.JsonAppender;

/**
 * @author taichi
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_mapred_json.erl">riak_kv_mapred_json.erl</a>
 */
public abstract class MapReducePhase implements JsonAppender<ArrayNode> {

	public enum PhaseType {
		map, reduce, link;
	}

	final PhaseType phase;

	final boolean keep;

	public MapReducePhase(PhaseType phase, boolean keep) {
		this.phase = phase;
		this.keep = keep;
	}

	@Override
	public void appendTo(ArrayNode json) {
		ObjectNode rootNode = json.addObject();
		ObjectNode phaseNode = rootNode.putObject(this.phase.name());
		appendPhase(phaseNode);
		phaseNode.put("keep", this.keep);
	}

	protected abstract void appendPhase(ObjectNode json);
}
