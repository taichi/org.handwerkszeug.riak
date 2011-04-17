package org.handwerkszeug.riak.op.mapreduce;

public interface MapReduceQuery {

	void addInputs(MapReduceInput... inputs);

	void addQueries(MapReducePhase... mapReducePhases);

	void setTimeout(int time);
}
