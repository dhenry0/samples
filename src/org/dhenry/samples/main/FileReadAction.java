package org.dhenry.samples.main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/** This is part of WordCounter */
public class FileReadAction extends RecursiveAction {

	private static final long serialVersionUID = 1L;
	private static final Logger log = Logger.getLogger(FileReadAction.class.getName());

	private Path path;
	private Map<String,LongAdder> counts;
	
	public FileReadAction(Path path, Map<String,LongAdder> counts) {
		this.path = path;
		this.counts = counts;
	}

	protected void compute() {
		Stream<String> stream = null;
		try {
			stream = Files.lines(path);
			stream.forEach(l -> invokeAll(new WordCountAction(l, counts)));
		} catch (IOException ex) {
			log.log(Level.WARNING, "", ex);
		} finally {
			if (stream != null) {
				stream.close();
			}
		}
	}
}
