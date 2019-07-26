package org.dhenry.samples.main;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CheckAnagrams {

	
	public static boolean compare(String word1, String word2) {
		Map<Character,SaneLongAdder> counts1 = getCharMap(word1);
		Map<Character,SaneLongAdder> counts2 = getCharMap(word2);
		if (counts1.size() != counts2.size())
			return false;
		// you can't do this with the stock LongAdder
		return counts1.equals(counts2);
	}
	
	public static Map<Character,SaneLongAdder> getCharMap(String word) {
		
		Map<Character,SaneLongAdder> counts = new ConcurrentHashMap<>();
		char[] chars = word.toCharArray();
		for (char c : chars) {
			counts.computeIfAbsent(Character.toLowerCase(c), v -> new SaneLongAdder()).increment();
		}
		return counts;
	}
	
	public static final void main(String[] args) {
		System.out.println(compare(args[0], args[1]));
	}
}


