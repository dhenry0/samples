package org.dhenry.samples.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import org.dhenry.samples.main.WordCountAction;
import org.dhenry.samples.main.WordCounter;
import org.junit.Test;

public class WordCountTests {

	public WordCountTests() {
		
	}

	@Test
	public void punctuationShouldBeRemoved() {
		WordCountAction action = new WordCountAction();
		assertEquals("word", action.stripPunctuation(".word,"));
		assertEquals("word", action.stripPunctuation("word"));
		assertEquals("word", action.stripPunctuation(".?word$$"));	
		assertEquals("word", action.stripPunctuation(".?word$$"));	
		assertEquals("", action.stripPunctuation("."));	
	}
	
	@Test
	public void mapSizeShouldIncrement() {
		WordCounter counter = new WordCounter();
		int oldSize = counter.getMapSize();
		counter.processLine("one two three");
		int newSize = counter.getMapSize();
		assertEquals(3, newSize - oldSize);
	}
	
	@Test
	public void wordCountShouldIncrement() {
		WordCounter counter = new WordCounter();
		counter.processLine("big bad bad wolf");
		long bigCount = counter.getCount("big");
		assertEquals(1, bigCount);
		long badCount = counter.getCount("bad");
		assertEquals(2, badCount);
		counter.processLine("bad bad");
		badCount = counter.getCount("bad");
		assertEquals(4, badCount);
	}
	
	/** for testing the full process of reading a file, this random Latin was 
	 * run through an online word count site and the distribution of words was copied
	 * out and massaged into the Map.toString format. Since the counts in the toString
	 * representations match, the test passes.
	 */
	@Test
	public void fileProcessingShouldWork() {
		WordCounter counter = new WordCounter();
		counter.processFile("../lorem_ipsum.txt");
		counter.awaitQuiescence();
		String actualMapString = counter.getMapString();
		String checkMapString = "";
		
		try {
			Optional<String> result = Files.lines(Paths.get("../lorem_ipsum_map.txt")).findFirst();
			checkMapString = result.orElse("");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		assertEquals(checkMapString, actualMapString);
	}
}
