package org.handwerkszeug.riak.model;

/**
 * 
 * @author taichi
 * @see <a
 *      href="https://github.com/basho/riak-erlang-client/blob/master/include/riakc_pb.hrl">riakc_pb.hrl</a>
 */
public class Quorum {

	static final int Q_One = Integer.MAX_VALUE - 1;
	static final int Q_Quorum = Integer.MAX_VALUE - 2;
	static final int Q_All = Integer.MAX_VALUE - 3;
	static final int Q_Default = Integer.MAX_VALUE - 4;

	public static final Quorum One = new Quorum(Q_One, "one");
	public static final Quorum Quorum = new Quorum(Q_Quorum, "quorum");
	public static final Quorum All = new Quorum(Q_All, "all");
	public static final Quorum Default = new Quorum(Q_Default, "default");

	final int quorum;
	final String string;

	Quorum(int quorum, String name) {
		this.quorum = quorum;
		this.string = name;
	}

	public int getInteger() {
		return this.quorum;
	}

	public String getString() {
		return this.string;
	}

	public static Quorum of(int quorum) {
		switch (quorum) {
		case Q_One:
			return One;
		case Q_Quorum:
			return Quorum;
		case Q_All:
			return All;
		case Q_Default:
			return Default;
		default:
			break;
		}
		if (0 < quorum) {
			return new Quorum(quorum, String.valueOf(quorum));
		}
		throw new IllegalArgumentException("quorum " + quorum);
	}
}