package org.handwerkszeug.riak.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * @author taichi
 * @see <a href="http://www.ietf.org/rfc/rfc1123.txt">RFC1123 - Requirements for
 *      Internet Hosts -- Application and Support</a>
 * @see <a href="http://www.ietf.org/rfc/rfc822.txt">RFC822 - STANDARD FOR THE
 *      FORMAT OF ARPA INTERNET TEXT MESSAGES</a>
 */
public class HttpUtil {

	/**
	 * RFC822 5. DATE AND TIME SPECIFICATION
	 */
	static final String RFC1123_DATEFORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
	static final TimeZone GMT = TimeZone.getTimeZone("GMT");

	public static DateFormat newGMTFormatter() {
		SimpleDateFormat fmt = new SimpleDateFormat(RFC1123_DATEFORMAT,
				Locale.ENGLISH);
		fmt.setTimeZone(GMT);
		return fmt;
	}

	public static String format(Date date) {
		DateFormat fmt = newGMTFormatter();
		return fmt.format(date);
	}

	public static Date parse(String date) {
		try {
			DateFormat fmt = newGMTFormatter();
			return fmt.parse(date);
		} catch (ParseException e) {
			throw new IllegalArgumentException(date);
		}
	}
}
