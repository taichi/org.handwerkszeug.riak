package org.handwerkszeug.riak.nls;

import org.handwerkszeug.riak.util.SingleLocaleStrings;

/**
 * @author taichi
 */
public class Messages {

	public static String UnknownMessageCode = "Unknown message code %s";

	public static String UnsupportedContentLength = "Unsupported content length. %s";

	public static String IllegalQuorum = "quorum %s";

	public static String IllegalClientId = "ClientId should be a 32-bit binary";

	public static String UnsupportedBucketProps = "Use REST API";

	public static String InputsMustSet = "inputs must set";

	public static String QueriesMustSet = "queries must set";

	public static String NoContents = "%s has no contents";

	public static String Receive = "{} receive {}";

	public static String SiblingExists = "{} {} Sibling exists";

	public static String LastModified = "Last-Modified: {}";

	public static String CloseChannel = "close channel";

	public static String SendTo = "{} send message to {}";

	public static String Queue = "{} is queued";

	public static String Finished = "{} is finished";

	public static String MapReduceResponseMustBeArray = "map/reduce response must be array but %s";

	static {
		SingleLocaleStrings.load(Messages.class);
	}
}
