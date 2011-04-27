package org.handwerkszeug.riak;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * unite host names for testing. <br/>
 * your environment is not {@code 127.0.0.1}, you also modify app.config.<br/>
 * there is a example. in this case, your riak server is {@code 192.168.0.2}.
 * 
 * <pre>
 * from {http, [ {"127.0.0.1", 8098 } ]},
 * to   {http, [ {"127.0.0.1", 8098 }, {"192.168.0.2",8098} ]},
 * 
 * from {pb_ip,    "127.0.0.1" },
 * to   {pb_ip,    "192.168.0.2" },
 * </pre>
 * 
 * @author taichi
 */
public class Hosts {

	static final Logger LOG = LoggerFactory.getLogger(Hosts.class);

	public static String RIAK_HOST = "127.0.0.1";

	public static int RIAK_PB_PORT = 8087;

	public static String RIAK_URL = "http://127.0.0.1:8098/riak";

	static {
		try {
			ClassLoader cl = Hosts.class.getClassLoader();
			Properties prop = new Properties();
			prop.load(cl.getResourceAsStream(Hosts.class.getName().replace('.',
					'/')
					+ ".properties"));
			RIAK_HOST = prop.getProperty("host", RIAK_HOST);
			RIAK_PB_PORT = Integer.valueOf(prop.getProperty("pb.port",
					String.valueOf(RIAK_PB_PORT)));

			StringBuilder stb = new StringBuilder();
			stb.append("http://");
			stb.append(RIAK_HOST);
			stb.append(":");
			stb.append(prop.getProperty("http.port", "8098"));
			stb.append("/riak");
			RIAK_URL = stb.toString();
		} catch (IOException e) {
			LOG.error(Markers.BOUNDARY, e.getMessage(), e);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug(Markers.BOUNDARY, "RIAK_HOST    {}", RIAK_HOST);
			LOG.debug(Markers.BOUNDARY, "RIAK_PB_PORT {}", RIAK_PB_PORT);
			LOG.debug(Markers.BOUNDARY, "RIAK_URL     {}", RIAK_URL);
		}
	}

	public static InetSocketAddress RIAK_ADDR = new InetSocketAddress(
			RIAK_HOST, RIAK_PB_PORT);

}
