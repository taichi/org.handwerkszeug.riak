package org.handwerkszeug.riak.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.handwerkszeug.riak.util.Streams.IORuntimeException;

/**
 * @author taichi
 */
public class JsonUtil {

	public static JsonNode read(InputStream in) {
		try {
			ObjectMapper om = new ObjectMapper();
			return om.readTree(new BufferedInputStream(in));
		} catch (IOException e) {
			throw new IORuntimeException(e);
		}
	}

	public static String getJsonPath(Class<?> clazz, String name) {
		StringBuilder stb = new StringBuilder();
		stb.append(clazz.getName().replace('.', '/'));
		stb.append("_");
		stb.append(name);
		stb.append(".json");
		return stb.toString();
	}

	public static JsonNode read(final Class<?> clazz, final String name) {
		final JsonNode[] result = new JsonNode[1];
		new Streams.using<InputStream, IOException>() {
			@Override
			public InputStream open() throws IOException {
				ClassLoader cl = clazz.getClassLoader();
				return cl.getResourceAsStream(getJsonPath(clazz, name));
			}

			@Override
			public void handle(InputStream stream) throws IOException {
				result[0] = read(stream);
			}

			@Override
			public void happen(IOException exception) {
				throw new IORuntimeException(exception);
			}
		};
		return result[0];
	}

	public static List<String> to(JsonNode node) {
		if (node != null && node.isArray()) {
			ArrayNode an = (ArrayNode) node;
			List<String> list = new ArrayList<String>(an.size());
			for (Iterator<JsonNode> i = an.getElements(); i.hasNext();) {
				String key = i.next().getValueAsText();
				list.add(key);
			}
			return list;
		}
		return Collections.emptyList();
	}

}
