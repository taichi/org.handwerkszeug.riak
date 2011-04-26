package org.handwerkszeug.riak.pbc;

import java.util.HashMap;
import java.util.Map;

import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.util.EnumUtil;
import org.handwerkszeug.riak.util.EnumUtil.VariableEnum;

import com.google.protobuf.MessageLite;

/**
 * <a href=
 * "https://github.com/basho/riak-erlang-client/blob/master/src/riakc_pb.erl"
 * >riakc_pb.erl</a>
 * 
 * @author taichi
 */
public enum MessageCodes implements VariableEnum {

	/** @see Riakclient.RpbErrorResp */
	RpbErrorResp(0, Riakclient.RpbErrorResp.getDefaultInstance()),
	/**  */
	RpbPingReq(1),
	/**  */
	RpbGetClientIdReq(3),
	/** @see Riakclient.RpbGetClientIdResp */
	RpbGetClientIdResp(4, Riakclient.RpbGetClientIdResp.getDefaultInstance()),
	/** @see Riakclient.RpbSetClientIdReq */
	RpbSetClientIdReq(5, Riakclient.RpbSetClientIdReq.getDefaultInstance()),
	/**  */
	RpbSetClientIdResp(6),
	/**  */
	RpbGetServerInfoReq(7),
	/** @see Riakclient.RpbGetServerInfoResp */
	RpbGetServerInfoResp(8, Riakclient.RpbGetServerInfoResp
			.getDefaultInstance()),
	/** @see Riakclient.RpbGetResp */
	RpbGetResp(10, Riakclient.RpbGetResp.getDefaultInstance()),
	/**  */
	RpbPutResp(12),
	/**  */
	RpbDelResp(14),
	/**  */
	RpbListBucketsReq(15),
	/** @see Riakclient.RpbListBucketsResp */
	RpbListBucketsResp(16, Riakclient.RpbListBucketsResp.getDefaultInstance()),
	/** @see Riakclient.RpbListKeysReq */
	RpbListKeysReq(17, Riakclient.RpbListKeysReq.getDefaultInstance()),
	/** @see Riakclient.RpbGetBucketReq */
	RpbGetBucketReq(19, Riakclient.RpbGetBucketReq.getDefaultInstance()),
	/** @see Riakclient.RpbGetBucketResp */
	RpbGetBucketResp(20, Riakclient.RpbGetBucketResp.getDefaultInstance()),
	/** @see Riakclient.RpbSetBucketReq */
	RpbSetBucketReq(21, Riakclient.RpbSetBucketReq.getDefaultInstance()),
	/**  */
	RpbSetBucketResp(22),
	/** @see Riakclient.RpbMapRedReq */
	RpbMapRedReq(23, Riakclient.RpbMapRedReq.getDefaultInstance());

	private final int code;
	private final MessageLite prototype;

	private static final Map<Class<?>, MessageCodes> map = new HashMap<Class<?>, MessageCodes>();
	static {
		for (MessageCodes mc : MessageCodes.values()) {
			if (mc.prototype != null) {
				map.put(mc.prototype.getClass(), mc);
			}
		}
	}

	MessageCodes(int code) {
		this(code, null);
	}

	MessageCodes(int code, MessageLite prototype) {
		this.code = code;
		this.prototype = prototype;
	}

	@Override
	public int getValue() {
		return this.code;
	}

	public MessageLite getPrototype() {
		return this.prototype;
	}

	public static MessageCodes valueOf(Class<?> clazz) {
		return map.get(clazz);
	}

	public static MessageCodes valueOf(int value) {
		MessageCodes mc = EnumUtil.find(MessageCodes.values(), value);
		if (mc == null) {
			throw new IllegalStateException(String.format(
					Messages.UnknownMessageCode, value));
		}
		return mc;
	}
}