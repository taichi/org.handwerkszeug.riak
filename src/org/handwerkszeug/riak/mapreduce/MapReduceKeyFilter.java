package org.handwerkszeug.riak.mapreduce;

import org.codehaus.jackson.node.ArrayNode;
import org.handwerkszeug.riak.util.JsonAppender;

/**
 * @author taichi
 */
public interface MapReduceKeyFilter extends JsonAppender<ArrayNode> {

}
