package org.dhenry.samples.main;

import java.util.Arrays;
import java.util.Random;

public class RandomNumberGenerator {

	private final Random random = new Random();
	private int[] arr = null;
	
	public RandomNumberGenerator(int[] randomNums, float[] probabilities) {
		
		if (randomNums == null || probabilities == null) {
			throw new IllegalStateException("input arrays cannot be null");
		}
		if (randomNums.length != probabilities.length) {
			throw new IllegalStateException("input array lengths must match");
		}
		int size = 0;
		for (int i = 0; i < probabilities.length; i++) {
			size += probabilities[i] * 100;
		}
		if (size != 100) {
			throw new IllegalStateException("probabilities must total 100%");
		}
		// Make an indexable structure with parts representing the relative size
		// of each probability.
		arr = new int[size];
		
		int p = 0;
		for (int i = 0; i < probabilities.length; i++) {
			for (int j= 1; j <= probabilities[i] * 100; j++) {
				arr[p++] = randomNums[i];
			}
		}
		// now there is a 100-element array containing repeating values of the randomNums elements.
		System.out.println("arr: " + Arrays.toString(arr));
	}

	public int nextNum() {
		// Get a random index into the array and return the value there that is from randomNums.
		// Don't cast to int too soon at least without more parens.
		float f = random.nextFloat() * 100f;
		int p = (int)f;
		return arr[p];
	}

	private static int[] parseIntArray(String str) {
		String[] strs = str.split(",");
		
		int[] arr = new int[strs.length];
		for (int i = 0; i < strs.length; i++) {
			arr[i] = Integer.parseInt(strs[i]);
		}
		return arr;
	}

	private static float[] parseFloatArray(String str) {
		String[] strs = str.split(",");
		
		float[] arr = new float[strs.length];
		for (int i = 0; i < strs.length; i++) {
			arr[i] = Float.parseFloat(strs[i]);
		}
		return arr;
	}

	public static void main(String[] args) {
		
		int[] randomNums = null;
		float[] probabilities = null;
		
		if (args == null || args.length == 0) {
			randomNums = new int[] {-1, 0, 1, 2, 3 };
			probabilities = new float[] {0.01f, 0.3f, 0.58f, 0.1f, 0.01f};
		} else {
			randomNums = parseIntArray(args[0]);
			probabilities = parseFloatArray(args[1]);
		}
		System.out.println("nums: " + Arrays.toString(randomNums));
		System.out.println("probs: " + Arrays.toString(probabilities));
		RandomNumberGenerator rng = new RandomNumberGenerator(randomNums, probabilities);
		for (int i = 1; i <= 100; i++) {
			int num = rng.nextNum();
			System.out.print(num + " ");
		}
	}
}
