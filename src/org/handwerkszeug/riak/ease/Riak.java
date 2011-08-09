package org.handwerkszeug.riak.ease;

import static org.handwerkszeug.riak.util.Validation.notNull;

import java.util.List;

import org.codehaus.jackson.node.ArrayNode;
import org.handwerkszeug.riak.RiakClient;
import org.handwerkszeug.riak.ease.internal.DefaultExceptionHandler;
import org.handwerkszeug.riak.ease.internal.MapReduceCommand;
import org.handwerkszeug.riak.mapreduce.MapReduceQueryBuilder;
import org.handwerkszeug.riak.mapreduce.internal.DefaultMapReduceQueryBuilder;
import org.handwerkszeug.riak.mapreduce.internal.MapReduceQueryContext;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.DefaultRiakObject;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.op.RiakOperations;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.util.CharsetUtil;

/**
 * @author taichi
 * @param <OP>
 */
public abstract class Riak<OP extends RiakOperations> {

	protected ExceptionHandler handler = DefaultExceptionHandler.INSTANCE;
	protected RiakClient<OP> client;

	protected Riak(RiakClient<OP> client) {
		this.client = client;
	}

	public void setExceptionHandler(ExceptionHandler handler) {
		notNull(handler, "handler");
		this.handler = handler;
	}

	public PingCommand<OP> ping() {
		return new PingCommand<OP>(this.client, this.handler);
	}

	public GetCommand<OP> get(Location location) {
		notNull(location, "location");
		return new GetCommand<OP>(this.client, this.handler, location);
	}

	public PostCommand<OP> post(RiakObject<byte[]> ro) {
		notNull(ro, "ro");
		return new PostCommand<OP>(this.client, this.handler, ro);
	}

	public PostCommand<OP> post(String bucket, String data) {
		notNull(bucket, "bucket");
		notNull(data, "data");
		DefaultRiakObject ro = new DefaultRiakObject(new Location(bucket, ""));
		ro.setContent(data.getBytes());
		return new PostCommand<OP>(this.client, this.handler, ro);
	}

	public PutCommand<OP> put(RiakObject<byte[]> ro) {
		notNull(ro, "ro");
		return new PutCommand<OP>(this.client, this.handler, ro);
	}

	public PutCommand<OP> put(Location location, String data) {
		notNull(location, "location");
		notNull(data, "data");
		DefaultRiakObject ro = new DefaultRiakObject(location);
		ro.setContent(data.getBytes());
		return put(ro);
	}

	public DeleteCommand<OP> delete(Location location) {
		notNull(location, "location");
		return new DeleteCommand<OP>(this.client, this.handler, location);
	}

	public ListKeysCommand<OP> listKeys(String bucket) {
		notNull(bucket, "bucket");
		return new ListKeysCommand<OP>(this.client, this.handler, bucket);
	}

	public GetBucketCommand<OP> getBucket(String bucket) {
		notNull(bucket, "bucket");
		return new GetBucketCommand<OP>(this.client, this.handler, bucket);
	}

	public SetBucketCommand<OP> setBucket(Bucket bucket) {
		notNull(bucket, "bucket");
		return new SetBucketCommand<OP>(this.client, this.handler, bucket);
	}

	public MapReduceQueryBuilder<List<ArrayNode>> mapReduce() {
		MapReduceQueryContext<List<ArrayNode>> context = new MapReduceQueryContext<List<ArrayNode>>() {
			@Override
			public List<ArrayNode> execute() {
				ChannelBuffer cb = ChannelBuffers.dynamicBuffer();
				prepare(new ChannelBufferOutputStream(cb));
				String query = cb.toString(CharsetUtil.UTF_8);
				MapReduceCommand<OP> cmd = new MapReduceCommand<OP>(
						Riak.this.client, Riak.this.handler, query);
				return cmd.execute();
			}
		};
		DefaultMapReduceQueryBuilder<List<ArrayNode>> builder = new DefaultMapReduceQueryBuilder<List<ArrayNode>>(
				context);
		builder.initialize();
		return builder;
	}

	public void dispose() {
		this.client.dispose();
	}
}
