package org.handwerkszeug.riak.mapreduce;

import org.handwerkszeug.riak.op.RiakResponse;

public interface MapReduceResponseHandler {

	void handle(RiakResponse<MapReduceResponse> response);

	void handleDone(RiakResponse<MapReduceResponse> response);
}
