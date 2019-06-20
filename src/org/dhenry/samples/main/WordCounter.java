package org.dhenry.samples.main;

import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

/** See WordCountAction.java, FileReadAction.java and WordCountTests.java too */
public class WordCounter {

	private static final Logger log = Logger.getLogger(WordCounter.class.getName());

	private final ForkJoinPool pool = new ForkJoinPool();
	private final Map<String,LongAdder> counts = new ConcurrentHashMap<>();

	public WordCounter() {
		
	}

	public void processFile(String filename) {
		pool.invoke(new FileReadAction(Paths.get(filename), counts));
	}

	public void awaitQuiescence() {
		pool.awaitQuiescence(5, TimeUnit.MINUTES);
	}

	/* following methods for testing */
	public void processLine(String line) {
		pool.invoke(new WordCountAction(line, counts));
	}

	public int getMapSize() {
		return counts.size();
	}

	public String getMapString() {
		return new TreeMap<String,LongAdder>(counts).toString();
	}

	public long getCount(String word) {
		LongAdder count = counts.get(word);
		if (count == null) {
			return 0;
		}
		return count.longValue();
	}

	public boolean hasCount(String word) {
		return counts.containsKey(word);
	}

	public static void main(String[] args) {
		WordCounter counter = new WordCounter();
		for (int i = 0; i < args.length; i++) {
			counter.processFile(args[i]);
		}
		counter.awaitQuiescence();
		log.info("counts: " + counter.getMapString());
	}
}
