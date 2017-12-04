package org.dhenry.samples.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.*;

import org.springframework.web.client.RestTemplate;

import org.dhenry.samples.main.SetSize;

public class SetTests {

	public int getSize() {
	  RestTemplate restTemplate = new RestTemplate();
	  SetSize setSize = restTemplate.getForObject("localhost:8080/size", SetSize.class);
      assertNotNull(setSize);
      return setSize.size();
	}
	
	public int add(String name) {
	  RestTemplate restTemplate = new RestTemplate();
	  restTemplate.post("localhost:8080/put", name);
	}
	
	public int remove(String name) {
	  RestTemplate restTemplate = new RestTemplate();
	  restTemplate.post("localhost:8080/remove", name);
	}
	
	@Test
	public void setAddingAndRemovingShouldWork() {
	   
		int emptySize = getSize();
		assertEquals(emptySize, 0);
		
		add("one");
		int oneSize = getSize();
		assertEquals(oneSize, 1);
		
		add("two");
		int twoSize = getSize();
		assertEquals(twoSize, 2);
		
		remove("one");
		int newSize = getSize();
		assertEquals(newSize, 1);
		
		add("two");
		newSize = getSize();
		assertEquals(twoSize, 1);
	}
}
