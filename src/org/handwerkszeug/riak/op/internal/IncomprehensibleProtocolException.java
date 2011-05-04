package org.handwerkszeug.riak.op.internal;

/**
 * received message is not proceed.<br/>
 * because that instance is incomprehensible.
 * 
 * @author taichi
 */
public class IncomprehensibleProtocolException extends IllegalStateException {

	private static final long serialVersionUID = 2017262668925362788L;

	final String procedure;

	public IncomprehensibleProtocolException(String procedure) {
		super(procedure);
		this.procedure = procedure;
	}

	public String getProcedure() {
		return this.procedure;
	}
}
