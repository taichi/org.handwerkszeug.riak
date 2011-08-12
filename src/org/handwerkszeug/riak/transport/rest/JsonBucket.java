package org.handwerkszeug.riak.transport.rest;

import java.util.List;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.map.annotate.JsonSerialize.Typing;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.Erlang;
import org.handwerkszeug.riak.model.Function;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.transport.rest.internal.FunctionJsonDeserializer;
import org.handwerkszeug.riak.transport.rest.internal.FunctionJsonSerializer;
import org.handwerkszeug.riak.transport.rest.internal.QuorumJsonDeserializer;
import org.handwerkszeug.riak.transport.rest.internal.QuorumJsonSerializer;

/**
 * @author taichi
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(JsonMethod.NONE)
@JsonSerialize(include = Inclusion.NON_NULL)
public class JsonBucket implements Bucket {

	@JsonProperty("name")
	String name;

	@JsonProperty("n_val")
	int numberOfReplicas;

	@JsonProperty("allow_mult")
	boolean allowMulti;

	@JsonProperty("last_write_wins")
	boolean lastWriteWins;

	@JsonSerialize(contentUsing = FunctionJsonSerializer.class, include = Inclusion.NON_NULL, contentAs = Function.class, typing = Typing.STATIC)
	@JsonDeserialize(contentUsing = FunctionJsonDeserializer.class, contentAs = Function.class)
	@JsonProperty("precommit")
	List<Function> precommits;

	@JsonSerialize(contentUsing = FunctionJsonSerializer.class, include = Inclusion.NON_NULL, contentAs = Erlang.class, typing = Typing.STATIC)
	@JsonDeserialize(contentUsing = FunctionJsonDeserializer.class, contentAs = Erlang.class)
	@JsonProperty("postcommit")
	List<Erlang> postcommits;

	@JsonSerialize(using = FunctionJsonSerializer.class, include = Inclusion.NON_NULL)
	@JsonDeserialize(using = FunctionJsonDeserializer.class)
	@JsonProperty("chash_keyfun")
	Erlang keyHashFunction;

	@JsonSerialize(using = FunctionJsonSerializer.class, include = Inclusion.NON_NULL)
	@JsonDeserialize(using = FunctionJsonDeserializer.class)
	@JsonProperty("linkfun")
	Erlang linkFunction;

	@JsonSerialize(using = QuorumJsonSerializer.class, include = Inclusion.NON_NULL)
	@JsonDeserialize(using = QuorumJsonDeserializer.class)
	@JsonProperty("r")
	Quorum defaultReadQuorum;

	@JsonSerialize(using = QuorumJsonSerializer.class, include = Inclusion.NON_NULL)
	@JsonDeserialize(using = QuorumJsonDeserializer.class)
	@JsonProperty("w")
	Quorum defaultWriteQuorum;

	@JsonSerialize(using = QuorumJsonSerializer.class, include = Inclusion.NON_NULL)
	@JsonDeserialize(using = QuorumJsonDeserializer.class)
	@JsonProperty("dw")
	Quorum defaultDurableWriteQuorum;

	@JsonSerialize(using = QuorumJsonSerializer.class, include = Inclusion.NON_NULL)
	@JsonDeserialize(using = QuorumJsonDeserializer.class)
	@JsonProperty("rw")
	Quorum defaultReadWriteQuorum;

	@JsonProperty("backend")
	String backend;

	public JsonBucket(@JsonProperty("name") String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public int getNumberOfReplicas() {
		return this.numberOfReplicas;
	}

	@Override
	public void setNumberOfReplicas(int nval) {
		this.numberOfReplicas = nval;
	}

	@Override
	public boolean getAllowMulti() {
		return this.allowMulti;
	}

	@Override
	public void setAllowMulti(boolean allow) {
		this.allowMulti = allow;
	}

	@Override
	public boolean getLastWriteWins() {
		return this.lastWriteWins;
	}

	@Override
	public void setLastWriteWins(boolean is) {
		this.lastWriteWins = is;
	}

	@Override
	public List<Function> getPrecommits() {
		return this.precommits;
	}

	@Override
	public void setPrecommits(List<Function> functions) {
		this.precommits = functions;
	}

	@Override
	public List<Erlang> getPostcommits() {
		return this.postcommits;
	}

	@Override
	public void setPostcommits(List<Erlang> functions) {
		this.postcommits = functions;
	}

	@Override
	public Erlang getKeyHashFunction() {
		return this.keyHashFunction;
	}

	@Override
	public void setKeyHashFunction(Erlang erlang) {
		this.keyHashFunction = erlang;
	}

	@Override
	public Erlang getLinkFunction() {
		return this.linkFunction;
	}

	@Override
	public void setLinkFunction(Erlang erlang) {
		this.linkFunction = erlang;
	}

	@Override
	public Quorum getDefaultReadQuorum() {
		return this.defaultReadQuorum;
	}

	@Override
	public void setDefaultReadQuorum(Quorum quorum) {
		this.defaultReadQuorum = quorum;
	}

	@Override
	public Quorum getDefaultWriteQuorum() {
		return this.defaultWriteQuorum;
	}

	@Override
	public void setDefaultWriteQuorum(Quorum quorum) {
		this.defaultWriteQuorum = quorum;
	}

	@Override
	public Quorum getDefaultDurableWriteQuorum() {
		return this.defaultDurableWriteQuorum;
	}

	@Override
	public void setDefaultDurableWriteQuorum(Quorum quorum) {
		this.defaultDurableWriteQuorum = quorum;
	}

	@Override
	public Quorum getDefaultReadWriteQuorum() {
		return this.defaultReadWriteQuorum;
	}

	@Override
	public void setDefaultReadWriteQuorum(Quorum quorum) {
		this.defaultReadWriteQuorum = quorum;
	}

	@Override
	public String getBackend() {
		return this.backend;
	}

	@Override
	public void setBackend(String name) {
		this.backend = name;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("JsonBucket [name=");
		builder.append(this.name);
		builder.append(", numberOfReplicas=");
		builder.append(this.numberOfReplicas);
		builder.append(", allowMulti=");
		builder.append(this.allowMulti);
		builder.append(", lastWriteWins=");
		builder.append(this.lastWriteWins);
		builder.append(", precommits=");
		builder.append(this.precommits);
		builder.append(", postcommits=");
		builder.append(this.postcommits);
		builder.append(", keyHashFunction=");
		builder.append(this.keyHashFunction);
		builder.append(", linkFunction=");
		builder.append(this.linkFunction);
		builder.append(", defaultReadQuorum=");
		builder.append(this.defaultReadQuorum);
		builder.append(", defaultWriteQuorum=");
		builder.append(this.defaultWriteQuorum);
		builder.append(", defaultDurableWriteQuorum=");
		builder.append(this.defaultDurableWriteQuorum);
		builder.append(", defaultReadWriteQuorum=");
		builder.append(this.defaultReadWriteQuorum);
		builder.append(", backend=");
		builder.append(this.backend);
		builder.append("]");
		return builder.toString();
	}

}