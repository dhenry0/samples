package org.dhenry.samples.main;

import java.util.Map;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;

/** This is part of WordCounter */
public class WordCountAction extends RecursiveAction {

	private static final long serialVersionUID = 1L;
	private static final int charThreshold = 
		Integer.getInteger("org.dhenry.samples.main.WordCountAction.charThreshold", 80);
	private static final char[] WHITESPACE = new char[] {' ', '\t', '\n', '\r'};
	private static Pattern wsPat = Pattern.compile("[ \\t\\n\\r]+");

	private String line;
	private Map<String,LongAdder> counts;
	
	// some methods public for testing
	public WordCountAction() {
		
	}

	public WordCountAction(String line, Map<String, LongAdder> counts) {
		this.line = line;
		this.counts = counts;
	}

	@Override
	protected void compute() {
		int length = line.length();
		if (length < charThreshold) {
			computeDirectly();
			return;
		}
		
		// back up to break after a whole word
		int split = length / 2;
		while (true) {
			char c = line.charAt(split);
			boolean isWs = false;
			for (int i = 0; i < WHITESPACE.length; i++) {
				if (c == WHITESPACE[i]) {
					isWs = true;
					break;
				}
			}
			if (isWs) {
				break;
			}
			split--;
			if (split == 0) {
				break; // sanity check
			}
		}
		String lh = line.substring(0, split + 1);
		String rh = line.substring(split);
		invokeAll(new WordCountAction(lh, counts),
				new WordCountAction(rh, counts));
		
	}
	
	// makes only one change on each end at most, none if possible
	public String stripPunctuation(String word) {
		int p = word.length();
		int startP = p;
		if (p == 0)
			return word;
		while (!Character.isAlphabetic(word.codePointAt(p - 1))) {
			p--;
			if (p == 0) {
				break;
			}
		}
		if (p < startP) {
			word = word.substring(0, p);
		}
		if (word.length() == 0) {
			return word;
		}
		p = 0;
		startP = p;
		while (!Character.isAlphabetic(word.codePointAt(p))) {
			p++;
			if (p == word.length() - 1) {
				break;
			}
		}
		if (p > startP) {
			word = word.substring(p);
		}
		return word;
	}
	
	protected void computeDirectly() {
		
		String[] words = wsPat.split(line); // reuse pattern unlike String.split()
		for (int i = 0; i < words.length; i++) {
			if (words[i].length() > 0) { // check for leading/trailing ws at the split
				// leading empty string is known issue with Pattern.split
				String word = stripPunctuation(words[i].toLowerCase());
				if (word.length() > 0) { // if was not just punctuation
					// puts a new LongAdder if one did not exist or returns existing LongAdder, increments either
					// atomic, may block a little
					counts.computeIfAbsent(word, v -> new LongAdder()).increment();
				}
			}
		}
	}

}
