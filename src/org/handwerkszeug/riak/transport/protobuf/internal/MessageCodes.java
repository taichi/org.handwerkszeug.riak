package org.handwerkszeug.riak.transport.protobuf.internal;

import java.util.HashMap;
import java.util.Map;

import org.handwerkszeug.riak.nls.Messages;
import org.handwerkszeug.riak.util.EnumUtil;
import org.handwerkszeug.riak.util.EnumUtil.VariableEnum;

import com.google.protobuf.MessageLite;

/**
 * @author taichi
 * @see <a href=
 *      "https://github.com/basho/riak-erlang-client/blob/master/src/riakc_pb.erl"
 *      >riakc_pb.erl</a>
 */
public enum MessageCodes implements VariableEnum {

	/** @see Riakclient.RpbErrorResp */
	RpbErrorResp(0, RawProtoBufRiakclient.RpbErrorResp.getDefaultInstance()),
	/**  */
	RpbPingReq(1),
	/**  */
	RpbPingResp(2),
	/**  */
	RpbGetClientIdReq(3),
	/** @see Riakclient.RpbGetClientIdResp */
	RpbGetClientIdResp(4, RawProtoBufRiakclient.RpbGetClientIdResp.getDefaultInstance()),
	/** @see Riakclient.RpbSetClientIdReq */
	RpbSetClientIdReq(5, RawProtoBufRiakclient.RpbSetClientIdReq.getDefaultInstance()),
	/**  */
	RpbSetClientIdResp(6),
	/**  */
	RpbGetServerInfoReq(7),
	/** @see Riakclient.RpbGetServerInfoResp */
	RpbGetServerInfoResp(8, RawProtoBufRiakclient.RpbGetServerInfoResp
			.getDefaultInstance()),
	/** @see Riakclient.RpbGetReq */
	RpbGetReq(9, RawProtoBufRiakclient.RpbGetReq.getDefaultInstance()),
	/** @see Riakclient.RpbGetResp */
	RpbGetResp(10, RawProtoBufRiakclient.RpbGetResp.getDefaultInstance()),
	/** @see Riakclient.RpbPutReq */
	RpbPutReq(11, RawProtoBufRiakclient.RpbPutReq.getDefaultInstance()),
	/** @see Riakclient.RpbPutResp */
	RpbPutResp(12, RawProtoBufRiakclient.RpbPutResp.getDefaultInstance()),
	/** @see Riakclient.RpbDelReq */
	RpbDelReq(13, RawProtoBufRiakclient.RpbDelReq.getDefaultInstance()),
	/**  */
	RpbDelResp(14),
	/**  */
	RpbListBucketsReq(15),
	/** @see Riakclient.RpbListBucketsResp */
	RpbListBucketsResp(16, RawProtoBufRiakclient.RpbListBucketsResp.getDefaultInstance()),
	/** @see Riakclient.RpbListKeysReq */
	RpbListKeysReq(17, RawProtoBufRiakclient.RpbListKeysReq.getDefaultInstance()),
	/** @see Riakclient.RpbListKeysResp */
	RpbListKeysResp(18, RawProtoBufRiakclient.RpbListKeysResp.getDefaultInstance()),
	/** @see Riakclient.RpbGetBucketReq */
	RpbGetBucketReq(19, RawProtoBufRiakclient.RpbGetBucketReq.getDefaultInstance()),
	/** @see Riakclient.RpbGetBucketResp */
	RpbGetBucketResp(20, RawProtoBufRiakclient.RpbGetBucketResp.getDefaultInstance()),
	/** @see Riakclient.RpbSetBucketReq */
	RpbSetBucketReq(21, RawProtoBufRiakclient.RpbSetBucketReq.getDefaultInstance()),
	/**  */
	RpbSetBucketResp(22),
	/** @see Riakclient.RpbMapRedReq */
	RpbMapRedReq(23, RawProtoBufRiakclient.RpbMapRedReq.getDefaultInstance()),
	/** @see Riakclient.RpbMapRedResp */
	RpbMapRedResp(24, RawProtoBufRiakclient.RpbMapRedResp.getDefaultInstance());

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