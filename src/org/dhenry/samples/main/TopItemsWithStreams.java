package org.dhenry.samples.main;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class TopItemsWithStreams {
	
	public List<String> getTopItems(String[] items, int n) {
	
		Map<String,LongAdder> m = new ConcurrentHashMap<>();
		for (String item : items) {
			m.computeIfAbsent(item, v -> new LongAdder()).increment();	
		}
		System.out.println("names: " + m);
		
		Comparator<Map.Entry<String, LongAdder>> byCount =
			    Comparator.comparingLong((Map.Entry<String, LongAdder> e) ->
			        e.getValue().longValue()).reversed();
		
		Comparator<Map.Entry<String,LongAdder>> byItem =
				Map.Entry.<String,LongAdder>comparingByKey().reversed();

		return m.entrySet().stream()
				.sorted(byCount.thenComparing(byItem))
				.limit(n)
				.map(e -> e.getKey())
				.collect(Collectors.toList());
	}

	public final static void main(String[] args) {
		String list = args[0];
		String[] items = list.split(",");
		int n = Integer.parseInt(args[1]);
		List<String> result = new TopItemsWithStreams().getTopItems(items, n);
		System.out.println(result);
	}
}

