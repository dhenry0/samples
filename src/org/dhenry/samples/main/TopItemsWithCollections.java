package org.dhenry.samples.main;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

public class TopItemsWithCollections {
	
	class Unit implements Comparable<Unit> {
		String item;
		int count;

		public Unit(String item) {
			this.item = item;
			count  = 1;
		}

		public boolean equals(Object o) {
			if (o instanceof Unit) {
				Unit other = (Unit)o;
				return item.equals(other.item);
			}
			return false;
		}

		public int hashCode() {
			return item.hashCode();
		}
		
		// don't need to have equals & hashcode in sync with compareTo for this usage of collections

		public int compareTo(Unit other) {
			int result = Integer.compare(count, other.count);
			if (result == 0)
				result = item.compareTo(other.item);
			System.out.println("compare " + this + " " + result + " " + other);
			return result;
		}

		public String toString() {
			return "<" + item + " " + count + ">";
		}
	}

	public List<String> getTopItems(String[] entries, int n) {
		
		List<String> output = new ArrayList<>();
		NavigableSet<Unit> counts = new TreeSet<>(Comparator.reverseOrder());
		Map<String,Unit> unitsByName = new HashMap<>();

		for (String entry : entries) {
			Unit newu = new Unit(entry);
			Unit oldu = unitsByName.get(entry);
			if (oldu == null) {
				counts.add(newu);
				unitsByName.put(entry, newu);
			} else {
				oldu.count++;
			}
		}

		// update order of set without TreeSet()'s optimization
		Set<Unit> tmpSet = new HashSet<>(counts);
		counts = new TreeSet<>(Comparator.reverseOrder());
		counts.addAll(tmpSet);

		System.out.println("counts: " + counts);
		System.out.println("names: " + unitsByName);
		int i = 0;
		for (Iterator<Unit> it = counts.iterator(); it.hasNext();) {
			Unit next = it.next();
			if (i >= n) {
				break;
			}
			i++;
			output.add(next.item);
			
		}
		return output;
	}
	
	public final static void main(String[] args) {
		String list = args[0];
		String[] items = list.split(",");
		int n = Integer.parseInt(args[1]);
		List<String> result = new TopItemsWithCollections().getTopItems(items, n);
		System.out.println(result);
	}
}
