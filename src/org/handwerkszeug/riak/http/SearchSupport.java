package org.handwerkszeug.riak.http;

/**
 * @author taichi
 */
public interface SearchSupport {

	/**
	 * @see <a href="http://wiki.basho.com/Riak-Search---Querying.html">Riak
	 *      Search Querying</a>
	 * @see <a
	 *      href="http://lucene.apache.org/java/2_4_0/queryparsersyntax.html">Apache
	 *      Lucene - Query Parser Syntax</a>
	 */
	void search(String query);
	// TODO temporally define. you need think about this.
}
