package org.handwerkszeug.riak;

import java.io.IOException;
import java.io.InputStream;
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

	public static int RIAK_HTTP_PORT = 8098;

	static {
		InputStream in = null;
		try {
			ClassLoader cl = Hosts.class.getClassLoader();
			in = cl.getResourceAsStream(Hosts.class.getName().replace('.', '/')
					+ ".properties");
			if (in != null) {
				Properties prop = new Properties();
				prop.load(in);
				RIAK_HOST = prop.getProperty("host", RIAK_HOST);
				RIAK_PB_PORT = Integer.valueOf(prop.getProperty("pb.port",
						String.valueOf(RIAK_PB_PORT)));

				RIAK_HTTP_PORT = Integer.valueOf(prop.getProperty("http.port",
						String.valueOf(RIAK_HTTP_PORT)));
			}
		} catch (IOException e) {
			LOG.error(Markers.BOUNDARY, e.getMessage(), e);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug(Markers.BOUNDARY, "RIAK_HOST      {}", RIAK_HOST);
			LOG.debug(Markers.BOUNDARY, "RIAK_PORT PB   {}", RIAK_PB_PORT);
			LOG.debug(Markers.BOUNDARY, "          HTTP {}", RIAK_HTTP_PORT);

		}
	}

}
