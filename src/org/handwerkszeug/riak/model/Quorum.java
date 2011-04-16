package org.handwerkszeug.riak.model;

/**
 * 
 * @author taichi
 * @see <a
 *      href="https://github.com/basho/riak-erlang-client/blob/master/include/riakc_pb.hrl">riakc_pb.hrl</a>
 */
public enum Quorum {
	One(Integer.MAX_VALUE - 1), Quorum(Integer.MAX_VALUE - 2), All(
			Integer.MAX_VALUE - 3), Default(Integer.MAX_VALUE - 4);

	final int quorum;

	Quorum(int quorum) {
		this.quorum = quorum;
	}

	public int getQuorum() {
		return this.quorum;
	}
}