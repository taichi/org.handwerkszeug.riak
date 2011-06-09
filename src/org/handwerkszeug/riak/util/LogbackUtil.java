package org.handwerkszeug.riak.util;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

/**
 * @author taichi
 */
public class LogbackUtil {

	public interface Action {
		void execute() throws Exception;
	}

	public static void suppressLogging(Action action, Class<?>... classes)
			throws Exception {
		suppressLogging(Level.INFO, action, classes);
	}

	public static void suppressLogging(Level newLevel, Action action,
			Class<?>... classes) throws Exception {
		LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
		List<Level> levels = new ArrayList<Level>();
		for (Class<?> c : classes) {
			Logger logger = lc.getLogger(c);
			Level current = logger.getLevel();
			levels.add(current);
			logger.setLevel(newLevel);
		}
		try {
			action.execute();
		} finally {
			for (int i = 0, l = levels.size(); i < l; i++) {
				Class<?> c = classes[i];
				Logger logger = lc.getLogger(c);
				logger.setLevel(levels.get(i));
			}
		}

	}
}