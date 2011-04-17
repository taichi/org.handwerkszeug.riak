package org.handwerkszeug.riak.model;

/**
 * @author taichi
 */
public interface Bucket {

	String getName();

	void setName(String name);

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

	void setPrecommit();
}
