package org.handwerkszeug.riak.model;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author taichi
 * @see <a
 *      href="https://github.com/basho/riak_kv/blob/master/src/riak_object.erl">riak_object.erl</a>
 */
public interface RiakObject<T> {

	Location getLocation();

	void setLocation(Location location);

	T getContent();

	String getVectorClock();

	void setVectorClock(String clock);

	String DEFAULT_CONTENT_TYPE = "text/plain";

	String getContentType();

	void setContentType(String contentType);

	String getCharset();

	void setCharset(String charset);

	String getContentEncoding();

	void setContentEncoding(String encoding);

	String getVtag();

	void setVtag(String vtag);

	List<Link> getLinks();

	void setLinks(List<Link> links);

	Date getLastModified();

	void setLastModified(Date date);

	Map<String, String> getUserMetadata();

	void setUserMetadata(Map<String, String> metadata);
}
