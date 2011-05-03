package org.handwerkszeug.riak.model;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.util.HashMap;
import java.util.Map;

import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.util.Validation;

/**
 * @author taichi
 * @see <a
 *      href="https://github.com/basho/riak-erlang-client/blob/master/include/riakc_pb.hrl">riakc_pb.hrl</a>
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_kv_wm_raw.erl">riak_kv_wm_raw.erl</a>
 */
public class Quorum {

	static final int Q_One = Integer.MAX_VALUE - 1;
	static final int Q_Quorum = Integer.MAX_VALUE - 2;
	static final int Q_All = Integer.MAX_VALUE - 3;
	static final int Q_Default = Integer.MAX_VALUE - 4;

	/**
	 * all nodes must respond
	 */
	public static final Quorum One = new Quorum(Q_One, "one");
	/**
	 * (n_val/2) + 1 nodes must respond.
	 */
	public static final Quorum Quorum = new Quorum(Q_Quorum, "quorum");
	/**
	 * equivalent to 1
	 */
	public static final Quorum All = new Quorum(Q_All, "all");

	public static final Quorum Default = new Quorum(Q_Default, "default");

	static final Map<String, Quorum> named = new HashMap<String, Quorum>(4);

	static {
		named.put(One.getString(), One);
		named.put(Quorum.getString(), Quorum);
		named.put(All.getString(), All);
		named.put(Default.getString(), Default);
	}

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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + quorum;
		result = prime * result + string.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		return equals((Quorum) obj);
	}

	public boolean equals(Quorum other) {
		return this.quorum == other.quorum && this.string.equals(other.string);
	}

	@Override
	public String toString() {
		return getString();
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
		throw new IllegalArgumentException(String.format(
				Messages.IllegalQuorum, quorum));
	}

	public static Quorum of(String quorum) {
		notNull(quorum, "quorum");
		Quorum result = named.get(quorum);
		if (result == null && Validation.isPositiveNumber(quorum)) {
			return of(Integer.parseInt(quorum));
		}
		return result;
	}

	public static boolean isNamed(Quorum quorum) {
		return quorum != null && named.containsKey(quorum.getString());
	}
}