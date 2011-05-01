package org.handwerkszeug.riak.http.rest;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.handwerkszeug.riak.Markers;
import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.http.HttpRiakOperations;
import org.handwerkszeug.riak.http.InputStreamHandler;
import org.handwerkszeug.riak.http.LinkCondition;
import org.handwerkszeug.riak.http.OutputStreamHandler;
import org.handwerkszeug.riak.mapreduce.MapReduceQueryConstructor;
import org.handwerkszeug.riak.mapreduce.MapReduceResponse;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.GetOptions;
import org.handwerkszeug.riak.model.KeyResponse;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.PutOptions;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakContentsResponse;
import org.handwerkszeug.riak.model.RiakFuture;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.RiakResponse;
import org.handwerkszeug.riak.model.internal.AbstractRiakResponse;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.op.SiblingHandler;
import org.handwerkszeug.riak.op.internal.CompletionSupport;
import org.handwerkszeug.riak.util.NettyUtil;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taichi
 */
public class RestRiakOperations implements HttpRiakOperations {

	static final Logger LOG = LoggerFactory.getLogger(RestRiakOperations.class);

	String riakURI;
	CompletionSupport support;

	public RestRiakOperations(String riakURI, Channel channel) {
		notNull(riakURI, "riakURI");
		notNull(channel, "channel");
		this.riakURI = removeSlashIfNeed(riakURI);
		this.support = new CompletionSupport(channel);
	}

	protected String removeSlashIfNeed(String uri) {
		return uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
	}

	@Override
	public RiakFuture ping(final RiakResponseHandler<String> handler) {
		HttpRequest request = build("/ping");
		return handle("ping", request, handler, new NettyUtil.MessageHandler() {
			@Override
			public boolean handle(Object receive) {
				if (receive instanceof HttpResponse) {
					handler.handle(new RestRiakResponse<String>() {
						public String getContents() {
							return "pong";
						};
					});
				}
				return true;
			}
		});
	}

	protected HttpRequest build(String path) {
		try {
			URI uri = new URI(this.riakURI + path);
			HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
					HttpMethod.GET, uri.toASCIIString());
			request.setHeader(HttpHeaders.Names.HOST, uri.getHost());
			// TODO if you want to connection pooling then remove this
			// statement.
			request.setHeader(HttpHeaders.Names.CONNECTION,
					HttpHeaders.Values.CLOSE);
			request.setHeader(HttpHeaders.Names.ACCEPT_ENCODING,
					HttpHeaders.Values.GZIP);
			return request;
		} catch (URISyntaxException e) {
			throw new RiakException(e);
		}
	}

	@Override
	public RiakFuture listBuckets(
			final RiakResponseHandler<List<String>> handler) {
		HttpRequest request = build("?buckets=true");
		return handle("listBuckets", request, handler,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							try {
								ChannelBuffer buffer = response.getContent();
								if (buffer.readable()) {
									ObjectMapper om = new ObjectMapper();
									JsonNode node = om
											.readTree(new ChannelBufferInputStream(
													buffer));
									if (node.isObject()) {
										ObjectNode on = (ObjectNode) node;
										JsonNode jn = on.get("buckets");
										if (jn != null && jn.isArray()) {
											ArrayNode an = (ArrayNode) jn;
											final List<String> list = new ArrayList<String>(
													an.size());
											for (Iterator<JsonNode> i = an
													.getElements(); i.hasNext();) {
												String key = i.next()
														.getValueAsText();
												list.add(key);
											}
											handler.handle(new RestRiakResponse<List<String>>() {
												@Override
												public List<String> getContents() {
													return list;
												}
											});
											return true;
										}
									}
								}
								throw new IllegalStateException();
							} catch (IOException e) {
								LOG.error(Markers.BOUNDARY, e.getMessage(), e);
								throw new RiakException(e);
							}
						}
						return true;
					}
				});
	}

	@Override
	public RiakFuture listKeys(String bucket,
			RiakResponseHandler<KeyResponse> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture getBucket(String bucket,
			RiakResponseHandler<Bucket> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture setBucket(Bucket bucket, RiakResponseHandler<_> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture get(Location location,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture get(Location location, GetOptions options,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture get(Location location, GetOptions options,
			SiblingHandler handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture put(RiakObject<byte[]> content,
			RiakResponseHandler<List<RiakObject<byte[]>>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture put(RiakObject<byte[]> content, PutOptions options,
			RiakResponseHandler<List<RiakObject<byte[]>>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture delete(Location location, RiakResponseHandler<_> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture delete(Location location, Quorum quorum,
			RiakResponseHandler<_> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture mapReduce(MapReduceQueryConstructor constructor,
			RiakResponseHandler<MapReduceResponse> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture mapReduce(String rawJson,
			RiakResponseHandler<MapReduceResponse> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture getStream(String key, GetOptions options,
			InputStreamHandler handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture putStream(RiakObject<OutputStreamHandler> content,
			RiakResponseHandler<String> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture walk(Location walkbegin, List<LinkCondition> conditions,
			RiakResponseHandler<List<RiakObject<byte[]>>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture getStats(RiakResponseHandler<ObjectNode> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	protected <T> RiakFuture handle(final String name, Object send,
			final RiakResponseHandler<T> users,
			final NettyUtil.MessageHandler internal) {
		return this.support.handle(name, send, users,
				new NettyUtil.MessageHandler() {
					@Override
					public boolean handle(Object receive) {
						if (receive instanceof HttpResponse) {
							HttpResponse response = (HttpResponse) receive;
							if (HttpResponseStatus.OK.equals(response
									.getStatus()) == false) {
								users.onError(new RestErrorResponse(response));
								return true;
							} else {
								return internal.handle(receive);
							}
						}
						return true;
					}
				});
	}

	abstract class RestRiakResponse<T> extends AbstractRiakResponse implements
			RiakContentsResponse<T> {
		@Override
		public void operationComplete() {
			complete();
		}
	}

	class RestErrorResponse implements RiakResponse {

		final HttpResponse master;

		public RestErrorResponse(HttpResponse master) {
			this.master = master;
		}

		@Override
		public int getResponseCode() {
			return this.master.getStatus().getCode();
		}

		@Override
		public String getMessage() {
			ChannelBuffer content = this.master.getContent();
			if (content.readable()) {
				return content.toString(CharsetUtil.UTF_8);
			}
			return "";
		}

		@Override
		public void operationComplete() {
			complete();
		}
	}

	protected void complete() {
		this.support.complete();
	}
}
