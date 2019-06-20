package org.dhenry.samples.main;

import java.util.ArrayList;
import java.util.List;

public class CountSquareNumbers {

	/* I found an article that illustrated the squares as groups of balls
	 *  and that made 2n + 1 make a lot of sense (w + h + corner)
	 */
	public List<Integer> countSquareNumbers(List<String> inputs) {
		List<Integer> results = new ArrayList<>();
		for (int i = 0; i < inputs.size(); i++) {
			String input = inputs.get(i);
			int p = input.indexOf(' ');
			int min = Integer.parseInt(input.substring(0, p));
			int max = Integer.parseInt(input.substring(p + 1));
						
			int count = 0;
			
			// have to find the first real square number
			int sqrt = 0;
			
			while (true) {
				int rem = min % 10;
				// wikipedia: In base 10, a square number can end only with 
				// digits 0, 1,  4, 5, 6 or  9
				//  Math.sqrt sourcecode says it can use hardware but maybe not
				// on hr env so do this check first
				if (rem == 2 || rem == 3 || rem == 7 || rem == 8) {
					// not a square
				} else {
					sqrt = (int)Math.sqrt(min);
					if (sqrt * sqrt == min) {
						break;
					}
				}
				min++;
			}

			// now jump by 2n + 1, starting with the last sqrt
			for (int j = min; j <= max; j += 2 * sqrt + 1) {
				count++;
				sqrt = (int)Math.sqrt(j);
			}
			results.add(count);
		}
		return results;
	}
}
