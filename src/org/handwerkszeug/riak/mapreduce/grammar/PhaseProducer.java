package org.handwerkszeug.riak.mapreduce.grammar;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.handwerkszeug.riak.model.Function;

/**
 * @author taichi
 */
public interface PhaseProducer<T> {

	ExecutablePhase<T> map(Function phase);

	ExecutablePhase<T> map(Function phase, Object arg);

	ExecutablePhase<T> map(Function phase, String arg);

	ExecutablePhase<T> map(Function phase, int arg);

	ExecutablePhase<T> map(Function phase, long arg);

	ExecutablePhase<T> map(Function phase, double arg);

	ExecutablePhase<T> map(Function phase, float arg);

	ExecutablePhase<T> map(Function phase, BigInteger arg);

	ExecutablePhase<T> map(Function phase, BigDecimal arg);

	ExecutablePhase<T> map(Function phase, boolean keep);

	ExecutablePhase<T> map(Function phase, Object arg, boolean keep);

	ExecutablePhase<T> map(Function phase, String arg, boolean keep);

	ExecutablePhase<T> map(Function phase, int arg, boolean keep);

	ExecutablePhase<T> map(Function phase, long arg, boolean keep);

	ExecutablePhase<T> map(Function phase, double arg, boolean keep);

	ExecutablePhase<T> map(Function phase, float arg, boolean keep);

	ExecutablePhase<T> map(Function phase, BigInteger arg, boolean keep);

	ExecutablePhase<T> map(Function phase, BigDecimal arg, boolean keep);

	ExecutablePhase<T> reduce(Function phase);

	ExecutablePhase<T> reduce(Function phase, Object arg);

	ExecutablePhase<T> reduce(Function phase, String arg);

	ExecutablePhase<T> reduce(Function phase, int arg);

	ExecutablePhase<T> reduce(Function phase, long arg);

	ExecutablePhase<T> reduce(Function phase, double arg);

	ExecutablePhase<T> reduce(Function phase, float arg);

	ExecutablePhase<T> reduce(Function phase, BigInteger arg);

	ExecutablePhase<T> reduce(Function phase, BigDecimal arg);

	ExecutablePhase<T> reduce(Function phase, boolean keep);

	ExecutablePhase<T> reduce(Function phase, Object arg, boolean keep);

	ExecutablePhase<T> reduce(Function phase, String arg, boolean keep);

	ExecutablePhase<T> reduce(Function phase, int arg, boolean keep);

	ExecutablePhase<T> reduce(Function phase, long arg, boolean keep);

	ExecutablePhase<T> reduce(Function phase, double arg, boolean keep);

	ExecutablePhase<T> reduce(Function phase, float arg, boolean keep);

	ExecutablePhase<T> reduce(Function phase, BigInteger arg, boolean keep);

	ExecutablePhase<T> reduce(Function phase, BigDecimal arg, boolean keep);

	ExecutablePhase<T> link(String bucket);

	ExecutablePhase<T> link(String bucket, String tag);

	ExecutablePhase<T> link(String bucket, boolean keep);

	ExecutablePhase<T> link(String bucket, String tag, boolean keep);
}
