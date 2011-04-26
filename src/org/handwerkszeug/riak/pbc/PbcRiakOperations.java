package org.handwerkszeug.riak.pbc;

import java.net.InetSocketAddress;
import java.util.List;

import org.handwerkszeug.riak.RiakException;
import org.handwerkszeug.riak._;
import org.handwerkszeug.riak.mapreduce.MapReduceQueryConstructor;
import org.handwerkszeug.riak.mapreduce.MapReduceResponseHandler;
import org.handwerkszeug.riak.model.Bucket;
import org.handwerkszeug.riak.model.Location;
import org.handwerkszeug.riak.model.Quorum;
import org.handwerkszeug.riak.model.RiakObject;
import org.handwerkszeug.riak.model.ServerInfo;
import org.handwerkszeug.riak.op.GetOptions;
import org.handwerkszeug.riak.op.KeyHandler;
import org.handwerkszeug.riak.op.PutOptions;
import org.handwerkszeug.riak.op.RiakFuture;
import org.handwerkszeug.riak.op.RiakOperations;
import org.handwerkszeug.riak.op.RiakResponseHandler;
import org.handwerkszeug.riak.op.SiblingHandler;
import org.handwerkszeug.riak.pbc.PbcRiakResponse.ErrorResponse;
import org.handwerkszeug.riak.pbc.PbcRiakResponse.NoOpResponse;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

public class PbcRiakOperations implements RiakOperations {

	ClientBootstrap bootstrap;

	String host;
	int port;

	public PbcRiakOperations(ClientBootstrap bootstrap, String host, int port) {
		this.bootstrap = bootstrap;
		this.host = host;
		this.port = port;
	}

	@Override
	public RiakFuture listBuckets(RiakResponseHandler<List<String>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture listKeys(String bucket, KeyHandler handler) {
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
	public RiakFuture get(Location key,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture get(Location key, GetOptions options,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture get(Location key, GetOptions options,
			SiblingHandler siblingHandler,
			RiakResponseHandler<RiakObject<byte[]>> handler) {
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
	public RiakFuture delete(Location key, RiakResponseHandler<_> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture delete(Location key, Quorum quorum,
			RiakResponseHandler<_> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void mapReduce(MapReduceQueryConstructor constructor,
			MapReduceResponseHandler handler) {
		// TODO Auto-generated method stub

	}

	@Override
	public RiakFuture ping(final RiakResponseHandler<_> handler) {
		ChannelFuture future = this.bootstrap.connect(new InetSocketAddress(
				this.host, this.port));
		Channel channel = future.awaitUninterruptibly().getChannel();
		ChannelPipeline pipeline = channel.getPipeline();
		final String NAME = "ping";
		pipeline.addLast(NAME, new SimpleChannelUpstreamHandler() {
			@Override
			public void messageReceived(ChannelHandlerContext ctx,
					MessageEvent e) throws Exception {
				ChannelPipeline pipeline = e.getChannel().getPipeline();
				pipeline.remove(NAME);
				try {
					Object o = e.getMessage();
					if (o instanceof Riakclient.RpbErrorResp) {
						Riakclient.RpbErrorResp error = (Riakclient.RpbErrorResp) o;
						handler.handle(new ErrorResponse(error));
					}
					if (MessageCodes.RpbPingResp.equals(o)) {
						handler.handle(new NoOpResponse() {
							@Override
							public String getMessage() {
								return "pong";
							};
						});
					}
				} finally {
					e.getChannel().close();
				}
			}
		});
		try {
			ChannelFuture cf = channel.write(MessageCodes.RpbPingReq);
			return new RiakFuture(cf);
		} catch (Exception e) {
			pipeline.remove(NAME);
			channel.close();
			throw new RiakException(e);
		}
	}

	@Override
	public RiakFuture getClientId(RiakResponseHandler<String> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture setClientId(String id, RiakResponseHandler<_> handler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RiakFuture serverInfo(RiakResponseHandler<ServerInfo> handler) {
		// TODO Auto-generated method stub
		return null;
	}

}
