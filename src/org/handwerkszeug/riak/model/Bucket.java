package org.handwerkszeug.riak.model;

import java.util.List;

/**
 * @author taichi
 */
public interface Bucket {

	String getName();

	/**
	 * the number of replicas for objects in this bucket
	 * 
	 * @return positive value
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
	List<Function> getPrecommits();

	/**
	 * @see <a href="http://wiki.basho.com/Pre--and-Post-Commit-Hooks.html">Pre
	 *      and Post Commit Hooks</a>
	 */
	void setPrecommits(List<Function> functions);

	/**
	 * @see <a href="http://wiki.basho.com/Pre--and-Post-Commit-Hooks.html">Pre
	 *      and Post Commit Hooks</a>
	 */
	List<Erlang> getPostcommits();

	/**
	 * @see <a href="http://wiki.basho.com/Pre--and-Post-Commit-Hooks.html">Pre
	 *      and Post Commit Hooks</a>
	 */
	void setPostcommits(List<Erlang> functions);

	Erlang getKeyHashFunction();

	void setKeyHashFunction(Erlang erlang);

	Erlang getLinkFunction();

	void setLinkFunction(Erlang erlang);

	/**
	 * how many replicas to read to before returning a successful response;<br/>
	 * default quorum values for operations on keys in the bucket.<br/>
	 * Valid values are:
	 * <ul>
	 * <li>all – all nodes must respond</li>
	 * <li>quorum – (n_val/2) + 1 nodes must respond. <b>This is the
	 * default</b>.</li>
	 * <li>one – equivalent to 1</li>
	 * <li>Any integer – must be less than or equal to n_val</li>
	 * <ul>
	 */
	Quorum getDefaultReadQuorum();

	void setDefaultReadQuorum(Quorum quorum);

	/**
	 * how many replicas to write to before returning a successful response;<br/>
	 * default quorum values for operations on keys in the bucket.<br/>
	 * Valid values are:
	 * <ul>
	 * <li>all – all nodes must respond</li>
	 * <li>quorum – (n_val/2) + 1 nodes must respond. <b>This is the
	 * default</b>.</li>
	 * <li>one – equivalent to 1</li>
	 * <li>Any integer – must be less than or equal to n_val</li>
	 * <ul>
	 */
	Quorum getDefaultWriteQuorum();

	void setDefaultWriteQuorum(Quorum quorum);

	/**
	 * how many replicas to commit to durable storage before returning a
	 * successful response;<br/>
	 * default quorum values for operations on keys in the bucket.<br/>
	 * Valid values are:
	 * <ul>
	 * <li>all – all nodes must respond</li>
	 * <li>quorum – (n_val/2) + 1 nodes must respond. <b>This is the
	 * default</b>.</li>
	 * <li>one – equivalent to 1</li>
	 * <li>Any integer – must be less than or equal to n_val</li>
	 * <ul>
	 */
	Quorum getDefaultDurableWriteQuorum();

	void setDefaultDurableWriteQuorum(Quorum quorum);

	/**
	 * quorum for both operations (get and put) involved in deleting an object.<br/>
	 * default quorum values for operations on keys in the bucket.<br/>
	 * Valid values are:
	 * <ul>
	 * <li>all – all nodes must respond</li>
	 * <li>quorum – (n_val/2) + 1 nodes must respond. <b>This is the
	 * default</b>.</li>
	 * <li>one – equivalent to 1</li>
	 * <li>Any integer – must be less than or equal to n_val</li>
	 * <ul>
	 */
	Quorum getDefaultReadWriteQuorum();

	void setDefaultReadWriteQuorum(Quorum quorum);

	/**
	 * when using riak_kv_multi_backend, which named backend to use for the
	 * bucket
	 */
	String getBackend();

	void setBackend(String name);
}
