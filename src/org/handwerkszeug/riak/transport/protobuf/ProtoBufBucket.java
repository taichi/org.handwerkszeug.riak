package org.handwerkszeug.riak.transport.protobuf;

import java.util.List;

import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.Erlang;
import org.handwerkszeug.riak.model.Function;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.nls.Messages;

public class ProtoBufBucket implements Bucket {

	final String name;

	int n_val;
	boolean allow_mult;

	public ProtoBufBucket(String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public int getNumberOfReplicas() {
		return this.n_val;
	}

	@Override
	public void setNumberOfReplicas(int nval) {
		this.n_val = nval;
	}

	@Override
	public boolean getAllowMulti() {
		return this.allow_mult;
	}

	@Override
	public void setAllowMulti(boolean allow) {
		this.allow_mult = allow;
	}

	@Override
	public boolean getLastWriteWins() {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public void setLastWriteWins(boolean is) {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public List<Function> getPrecommits() {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public void setPrecommits(List<Function> functions) {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public List<Erlang> getPostcommits() {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public void setPostcommits(List<Erlang> functions) {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public Erlang getKeyHashFunction() {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public void setKeyHashFunction(Erlang erlang) {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public Erlang getLinkFunction() {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public void setLinkFunction(Erlang erlang) {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public Quorum getDefaultReadQuorum() {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public void setDefaultReadQuorum(Quorum quorum) {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public Quorum getDefaultWriteQuorum() {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public void setDefaultWriteQuorum(Quorum quorum) {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public Quorum getDefaultDurableWriteQuorum() {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public void setDefaultDurableWriteQuorum(Quorum quorum) {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public Quorum getDefaultReadWriteQuorum() {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public void setDefaultReadWriteQuorum(Quorum quorum) {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public String getBackend() {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

	@Override
	public void setBackend(String name) {
		throw new UnsupportedOperationException(Messages.UnsupportedBucketProps);
	}

}
