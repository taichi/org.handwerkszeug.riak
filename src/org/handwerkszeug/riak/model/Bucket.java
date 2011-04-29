package org.handwerkszeug.riak.model;

/**
 * @author taichi
 */
public interface Bucket {

	String getName();

	/**
	 * the number of replicas for objects in this bucket
	 * 
	 * @return value > 0
	 */
	int getNumberOfReplicas();

	void setNumberOfReplicas(int nval);

	/**
	 * whether to allow sibling objects to be created (concurrent updates)
	 */
	boolean getAllowMulti();

	void setAllowMulti(boolean allow);

	/**
	 * whether to ignore object history (vector clock) when writing
	 */
	boolean getLastWriteWins();

	void setLastWriteWins(boolean is);

	/**
	 * @see <a href="http://wiki.basho.com/Pre--and-Post-Commit-Hooks.html">Pre
	 *      and Post Commit Hooks</a>
	 */
	void setPrecommit(Function erlang);

	void setPostcommit(Erlang erlang);

	void setKeyHashFunction(Erlang erlang);

	void setLinkFunction(Erlang erlang);

	/**
	 * how many replicas need to agree when retrieving the object; possible
	 * values include ‘default’, ‘one’, ‘quorum’, ‘all’, or any integer <= N
	 * (default is defined per the bucket)
	 */
	Quorum getDefaultReadQuorum();

	void setDefaultReadQuorum(Quorum quorum);

	/**
	 * how many replicas to write to before returning a successful response;
	 * possible values include ‘default’, ‘one’, ‘quorum’, ‘all’, or any integer
	 * <= N (default is defined per the bucket)
	 */
	Quorum getDefaultWriteQuorum();

	void setDefaultWriteQuorum(Quorum quorum);

	/**
	 * how many replicas to commit to durable storage before returning a
	 * successful response; possible values include ‘default’, ‘one’, ‘quorum’,
	 * ‘all’, or any integer <= N (default is defined per the bucket)
	 */
	Quorum getDefaultDurableWriteQuorum();

	void setDefaultDurableWriteQuorum(Quorum quorum);

	/**
	 * quorum for both operations (get and put) involved in deleting an object.
	 * (default is set at the bucket level)
	 */
	Quorum getDefaultReadWriteQuorum();

	void setDefaultReadWriteQuorum();

	/**
	 * when using riak_kv_multi_backend, which named backend to use for the
	 * bucket
	 */
	String getBackend();

	void setBackend(String name);
}
