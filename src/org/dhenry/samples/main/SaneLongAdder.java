package org.dhenry.samples.main;

import java.io.Serializable;
import java.util.concurrent.atomic.LongAdder;

/**
 * To do anything useful in Collections with LongAdder, these methods are needed.
 */
public class SaneLongAdder extends LongAdder implements Serializable, Comparable<SaneLongAdder> {

	private static final long serialVersionUID = 1L;
	protected long cachedSum = -1;

	public SaneLongAdder() {
		super();
	}

	public void increment() {
		super.increment();
		cachedSum = -1;
	}

	public void decrement() {
		super.decrement();
		cachedSum = -1;
	}

	public void add(long x) {
		super.add(x);
		cachedSum = -1;
	}

	public long sum() {
		if (cachedSum == -1) {
			cachedSum = super.sum();
		}
		return cachedSum;
	}

	public int hashCode() {
		return Long.hashCode(sum());
	}

	public boolean equals(Object o) {
		if (o instanceof SaneLongAdder) {
			SaneLongAdder other = (SaneLongAdder)o;
			return sum() == other.sum();
		}
		return false;
	}

	public int compareTo(SaneLongAdder other) {
		return Long.compare(sum(), other.sum());
	}
}
