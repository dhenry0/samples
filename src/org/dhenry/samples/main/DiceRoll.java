package org.dhenry.samples.main;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * pass the count and URL to the bin on the command line.
 * http://requestb.in couldn't be found so I used http://requestbin.net
 * 
 * USAGE: java org.dhenry.samples.main.DiceRoll 10 http://requestbin.net/r/1ip0j6u1
 * 
 * Question 2: to scale this up I would do:
 *   Cache the regex Pattern or maybe switch to String.indexOf
 *   Use a better http client like Apache Commons
 *   Make sure HTTP KeepAlive is activated
 *   Parallelize multiple post tasks with a thread pool
 *   See if async I/O can be used to reduce threads, increase performance
 *   
 *   Notes about this implementation:
 *   I know about HTML DOM parsers but I wanted something simpler and faster
 *   I didn't want to use any other libraries like for JSON.
 */
public class DiceRoll {

	private static final Logger log = Logger.getLogger(DiceRoll.class.getName());

	private int diceRolls;
	private String binUrl;

	public DiceRoll(int diceRolls, String binUrl) {
		this.diceRolls = diceRolls;
		this.binUrl = binUrl;
	}
	
	public void run() {
		int[] rolls = rollDice();
		if (rolls == null) {
			return; // error was logged
		}
		int[] sortedRolls = printAndSortRolls(rolls);
		postToBin(sortedRolls, binUrl);
	}
	
	private int[] rollDice() {
		
		String urlStr = "https://www.random.org/dice/?num=" + diceRolls;
		String responseContent = httpRequest(urlStr, null);
		if (responseContent == null) {
			return null;
		}
		try {
			return parseDiceOutput(responseContent);
		} catch (IOException ex) {
			return null;
		}
	}
	
	private int[] parseDiceOutput(String content) throws IOException {
		//                           <img src="dice1.png" alt="1" />
		Pattern p = Pattern.compile("<img src=\"dice..png\" alt=\"(.)\" />");
	    Matcher m = p.matcher(content);

	    int[] result = new int[diceRolls];
	    int i = 0;
	    
	    while (m.find()) {
	    	String face = m.group(1);
	    	result[i++] = Integer.parseInt(face);
	    }
	    log.fine(i + " matches found");

	    return result;
	}
	
	private int[] printAndSortRolls(int[] rolls) {
		
		int[] buckets = new int[7];
		for (int i = 0; i < rolls.length; i++) {
			buckets[rolls[i]]++;
		}
		for (int i = 1; i <= 6; i++) {
			System.out.println(i + " -> " + buckets[i]);
		}
		Arrays.sort(rolls);
		System.err.println(Arrays.toString(rolls));
		return rolls;
	}
	
	private void postToBin(int[] sortedRolls, String binUrl) {
		
		String content = "{ \"dice\": " + Arrays.toString(sortedRolls) + "}";
		httpRequest(binUrl, content);
	}
	
	private String httpRequest(String urlStr, String requestContent) {
		
		URL url = null;
		
		try {
			url = new URL(urlStr);
		} catch (MalformedURLException ex) {
			log.log(Level.WARNING, "url", ex);
			return null;
		}

		HttpURLConnection conn = null;

		try {
			conn = (HttpURLConnection) url.openConnection();

			boolean isPost = requestContent != null;
			conn.setRequestMethod(isPost ? "POST" : "GET");
			conn.setConnectTimeout(5000);
			conn.setReadTimeout(5000);
			conn.addRequestProperty("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3");
			conn.addRequestProperty("accept-encoding", "gzip");
			conn.addRequestProperty("accept-language", "en-US,en;q=0.9");
			conn.addRequestProperty("cache-control", "max-age=0");
			conn.addRequestProperty("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36");
			
			if (isPost) {
				conn.setDoOutput(true);
				try (DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
					out.writeBytes(requestContent);
				} catch (IOException ex) {
					log.log(Level.WARNING, "write", ex);
				}
			}
			int status = conn.getResponseCode();
	
			if (status >= 400) {
				throw new IOException("http error status: " + status);
			} else if (status >= 300) {
				throw new IOException("http unhandled redirect: " + status);
			}

			try (BufferedReader in = new BufferedReader(
					new InputStreamReader(new GZIPInputStream(conn.getInputStream())))) {

				String inputLine = null;

				StringBuilder content = new StringBuilder();
				while ((inputLine = in.readLine()) != null) {
				    content.append(inputLine);
				}
				return content.toString();
			} catch (IOException ex) {
				log.log(Level.WARNING, "reader", ex);
				return null;
			}

		} catch (IOException ex) {
			log.log(Level.WARNING, "connection", ex);
			return null;
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
	}
	
	public static final void main(String[] args) {
		int rolls = Integer.parseInt(args[0]);
		String binUrl = args[1];
		
		DiceRoll dr = new DiceRoll(rolls, binUrl);
		dr.run();
	}
}
