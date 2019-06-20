
package org.dhenry.samples.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.dhenry.samples.main.RandomNumberGenerator;
import org.junit.Test;

public class RandomNumberTests {

	public RandomNumberTests() {
		
	}

	@Test
	public void probabilitiesOver100PercentShouldThrowException() {
		RandomNumberGenerator rng = null;
		Exception exx = null;
		try {
		  rng = new RandomNumberGenerator(new int[] {1,2}, new float[] {.50f,.51f});
		} catch (IllegalStateException ex) {
			exx = ex;
		}
		assertNotNull(exx);
	}

	@Test 
	public void unequalArrayLengthsShouldThrowException() {
		RandomNumberGenerator rng = null;
		Exception exx = null;
		try {
		  rng = new RandomNumberGenerator(new int[] {1,2,3,4}, new float[] {.33f,.33f, .34f});
		} catch (IllegalStateException ex) {
			exx = ex;
		}
		assertNotNull(exx);
	}

	@Test
	public void nullParametersShouldThrowException() {
		RandomNumberGenerator rng = null;
		Exception exx = null;
		try {
		  rng = new RandomNumberGenerator(null, new float[] {.50f,.50f});
		} catch (IllegalStateException ex) {
			exx = ex;
		}
		assertNotNull(exx);
	}

	@Test
	public void moreThanOneValueShouldBeReturned() {
		int[] counters = new int[2];
		RandomNumberGenerator rng = new RandomNumberGenerator(new int[] {0,1}, new float[] {.50f,.50f});
		for (int i = 1; i <= 100; i++) {
			int next = rng.nextNum();
			counters[next]++;
		}
		System.out.println("50% results: " + Arrays.toString(counters));
		assertTrue(counters[0] >= 49);
		assertTrue(counters[1] >= 49);
	}
}
