package examples;

import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.and;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.endsWith;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.filters;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.js;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.location;
import static org.handwerkszeug.riak.mapreduce.MapReduceQuerySupport.startsWith;

import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.handwerkszeug.riak.model.JavaScript;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.transport.protobuf.ProtoBufRiak;

/**
 * @author taichi
 */
public class EaseExamples {

	static final String DEFALUT_HOST = "localhost";

	public static void main(String[] args) {
		String host = DEFALUT_HOST;
		if (args != null && 0 < args.length) {
			host = args[0];
		}

		ProtoBufRiak riak = ProtoBufRiak.create(host);
		try {
			EaseExamples ee = new EaseExamples();
			ee.basicOperations(riak);
			ee.mapReduce(riak);
		} finally {
			riak.dispose();
		}
	}

	public void basicOperations(ProtoBufRiak riak) {
		Location location = new Location("exampleBucket", "key");
		riak.put(location, "test data").setReturnBody(false).execute();

		RiakObject<byte[]> ro = riak.get(location).execute();
		System.out.println(new String(ro.getContent()));
		System.out.println(ro);

		riak.delete(location).execute();

	}

	public void mapReduce(ProtoBufRiak riak) {
		setUp(riak);

		simpleMapReduce(riak);

		keyFiltersMapReduce(riak);

		adHocSourceMapReduce(riak);

		delete(riak);
	}

	void simpleMapReduce(ProtoBufRiak riak) {
		List<ArrayNode> list = riak.mapReduce().inputs("alice")
				.map(JavaScript.mapValues, true).execute();
		printResult(list);
	}

	void keyFiltersMapReduce(ProtoBufRiak riak) {
		List<ArrayNode> list = riak
				.mapReduce()
				.inputs("alice")
				.keyFilters(
						and(filters(startsWith("p")), filters(endsWith("5"))))
				.map(JavaScript.mapValues, true).execute();
		printResult(list);
	}

	/**
	 * @see <a
	 *      href="http://wiki.basho.com/MapReduce.html#MapReduce-via-the-HTTP-API">MapReduce
	 *      via the HTTP API</a>
	 * */
	void adHocSourceMapReduce(ProtoBufRiak riak) {
		List<ArrayNode> list = riak
				.mapReduce()
				.inputs(location("alice", "p1"), location("alice", "p2"),
						location("alice", "p5"))
				.map(js("function(v) {" //
						+ "  var m = v.values[0].data.toLowerCase().match(/\\w*/g);" //
						+ "  var r = [];" //
						+ "  for(var i in m) {" //
						+ "    if(m[i] != '') {" //
						+ "      var o = {};" //
						+ "      o[m[i]] = 1;" //
						+ "      r.push(o);" //
						+ "    }" //
						+ "  }" //
						+ "  return r;" //
						+ "}")).reduce( //
						js("function(v) {" //
								+ "  var r = {};" //
								+ "  for(var i in v) {" //
								+ "    for(var w in v[i]) {" //
								+ "      if(w in r) r[w] += v[i][w];" //
								+ "      else r[w] = v[i][w];" //
								+ "    }" //
								+ "  }" //
								+ "  return [r];" //
								+ "}")).execute();

		for (ArrayNode an : list) {
			JsonNode jn = an.get(0);
			System.out.println("****************************");
			System.out.println(jn);
			System.out.println("****************************");
		}
	}

	void setUp(ProtoBufRiak riak) {
		Location p1 = new Location("alice", "p1");
		riak.put(
				p1,
				"Alice was beginning to get very tired of sitting by her sister on the "
						+ "bank, and of having nothing to do: once or twice she had peeped into the "
						+ "book her sister was reading, but it had no pictures or conversations in "
						+ "it, 'and what is the use of a book,' thought Alice 'without pictures or "
						+ "conversation?'").execute();
		Location p2 = new Location("alice", "p2");
		riak.put(
				p2,
				"So she was considering in her own mind (as well as she could, for the "
						+ "hot day made her feel very sleepy and stupid), whether the pleasure "
						+ "of making a daisy-chain would be worth the trouble of getting up and "
						+ "picking the daisies, when suddenly a White Rabbit with pink eyes ran "
						+ "close by her.").execute();
		Location p5 = new Location("alice", "p5");
		riak.put(
				p5,
				"The rabbit-hole went straight on like a tunnel for some way, and then "
						+ "dipped suddenly down, so suddenly that Alice had not a moment to think "
						+ "about stopping herself before she found herself falling down a very deep "
						+ "well.").execute();
	}

	void printResult(List<ArrayNode> list) {
		for (ArrayNode an : list) {
			JsonNode jn = an.get(0);
			System.out.println("****************************");
			System.out.println(jn.getTextValue());
			System.out.println("****************************");
		}
	}

	void delete(ProtoBufRiak riak) {
		riak.delete(new Location("alice", "p1")).execute();
		riak.delete(new Location("alice", "p2")).execute();
		riak.delete(new Location("alice", "p5")).execute();
	}
}
