package org.dhenry.samples.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONObject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;

public class UniqueEventTracker {

	public static final Logger log = Logger.getLogger(UniqueEventTracker.class.getName());
	public static int PERIOD_HOUR = 1;
	public static int PERIOD_DAY = 2;
	public static int PERIOD_WEEK = 3;
	public static int PERIOD_MONTH = 4;
	
	private volatile int currentHour = -1;
	private volatile int currentDay = -1;
	private volatile int dayValue = -1;
	private volatile int currentMonth = -1;
	private volatile int monthValue = -1;
	private volatile int currentWeek = -1;
	private volatile int weekValue = -1;
	private int maxHours = 10; // actual limits read from settings
	private int maxDays = 10;
	private int maxWeeks = 10;
	private int maxMonths = 10;
	private int oldestHour = -1;
	private int oldestDay = -1;
	private int oldestWeek = -1;
	private int oldestMonth = -1;
	
	private boolean running;
	private CassandraConnection conn;
	private Deque<Event> queue = new ConcurrentLinkedDeque<>();
	private Processor processor = new Processor();
	
	/**
	 * This system keeps historical events from several time ranges in order
	 * to allow efficient searching for a matching event. The time ranges
	 * are implemented as tables in Cassandra, which can be deleted to
	 * implement efficient rolloff of old data.
	 * 
	 * @param cassandraDc (datacenter name)
	 * @param cassandraContactPt (host or address)
	 */
	public UniqueEventTracker(String cassandraDc, String cassandraContactPt) {
		
		conn = new CassandraConnection();
		conn.connect(cassandraDc, cassandraContactPt);

		loadSettings();
		processor.start();
		setupTimeSeriesRoll();
	}

	/**
	 * @return boolean if queuing was successful
	 */
	public boolean processEvent(Event event) {
		boolean queued = queue.offer(event);
		if (processor.waiting) {
			synchronized (processor) {
				processor.notify();
			}
		}
		return queued;
	}
	
	/**
	 * 
	 * @param value: probably a url, denormalized from properties
	 * @param actorUuid added because other params are not specific enough
	 * @param dimension is bit-mapped e.g. 00001010 & 00101010 matches
	 * @param periodicityType one of the public static period constants
	 * @param PeriodicityValue is how many back.
	 * @return the count of matching records found.
	 */
	public int getTotalUniques(String type, String tenantId, int periodicityType,
			int periodicityValue, String actorUuid, int dimensions, String value) throws SQLException {
		
		int currentPeriodicity = getCurrentPeriodicity(periodicityType);
		
		String cql = "select source_api_key, source_remote_ip, tenant_id, actor_uuid, type, "
			+ "properties, dimensions, value, occurrence_time, ingestion_time "
			+ "from " + makeTableName(periodicityType, currentPeriodicity-periodicityValue)
			+ "where type = ? and tenant_id = ? and value = ? and actor_uuid = ?";

		try {
			Session sess = conn.getSession();
			PreparedStatement ps = sess.prepare(cql);
			BoundStatement bs = ps.bind(type, tenantId, value, actorUuid);
			ResultSet rs = sess.execute(bs);
			int count = 0;
			Row row = null;
			while ((row = rs.one()) != null) {
				// cassandra only has >, >=, =, <, <= (indexable) operators
				int dimensionsFromEvent = row.getInt("dimensions");
				if ((dimensions & dimensionsFromEvent) != 0) {
					count++;
				}
			}
			return count;
		} catch (DriverException ex) {
			log.log(Level.WARNING, "", ex);
			throw new SQLException(ex.getMessage());
		}
	}
	
	/**
	 *
	 * @param actorUuid added because other params are not specific enough
	 * @param periodicityType one of the public static period constants
	 * @param PeriodicityValue is how many back.
	 * @return matching records.
	 */
	List<Event> checkUniques(String type, String tenantId,
			int periodicityType, int periodicityValue, String actorUuid,
			List<String> possible_values)
			throws SQLException {

		List<Event> results = new ArrayList<>();
		int currentPeriodicity = getCurrentPeriodicity(periodicityType);
		
		String cql = "select source_api_key, source_remote_ip, tenant_id, actor_uuid, type, "
			+ "properties, dimensions, value, occurrence_time, ingestion_time "
			+ "from " + makeTableName(periodicityType, currentPeriodicity-periodicityValue)
			+ "where type = ? and tenant_id = ? and actor_uuid = ? and value in ?";

		try {
			Session sess = conn.getSession();
			PreparedStatement ps = sess.prepare(cql);
			BoundStatement bs = ps.bind(type, tenantId, actorUuid, possible_values);
			ResultSet rs = sess.execute(bs);
			Row row = null;
			while ((row = rs.one()) != null) {
				Event e = new Event(row.getString("source_api_key"),
					row.getString("source_remote_ip"),
					row.getString("tenant_id"),
					row.getString("actor_uuid"),
					row.getString("type"),
					row.getString("properties"), 
					row.getString("occurrence_time"),
					row.getString("ingestion_time"),
					row.getInt("dimensions"));
				results.add(e);
			}
		} catch (DriverException ex) {
			log.log(Level.WARNING, "", ex);
			throw new SQLException(ex.getMessage());
		}
		return results;
	}
	
	private void loadSettings() {
		Properties properties = new Properties();
		FileInputStream fin = null;
		String filename = "events.settings";
		if (new File(filename).exists()) {
			try {
				fin = new FileInputStream(filename);
				properties.load(fin);
			} catch (IOException ex) {
			    log.log(Level.WARNING, "error loading events.settings", ex);
			} finally {
				 if (fin != null) {
					 try {
						 fin.close();
					 } catch (IOException ex2) {}
				}
			}
		} else {
			maxHours = 24;
			maxDays = 7;
			maxWeeks = 5;
			maxMonths = 12;
		}
		currentHour = getIntProperty("currentHour", properties, 0);
		currentDay = getIntProperty("currentDay", properties, 0);
		currentWeek = getIntProperty("currentWeek", properties, 0);
		currentMonth = getIntProperty("currentMonth", properties, 0);
		maxHours = getIntProperty("maxHours", properties, maxHours);
		maxDays = getIntProperty("maxDays", properties, maxDays);
		maxWeeks = getIntProperty("maxWeeks", properties, maxWeeks);
		maxMonths = getIntProperty("maxMonths", properties, maxMonths);
		oldestHour = getIntProperty("oldestHour", properties, 0);
		oldestDay = getIntProperty("oldestDay", properties, 0);
		oldestWeek = getIntProperty("oldestWeek", properties, 0);
		oldestMonth = getIntProperty("oldestMonth", properties, 0);
	}

	private int getIntProperty(String name, Properties props, int defaultValue) {
		String value = props.getProperty(name);
		if (value == null) {
			return defaultValue;
		}
		try {
		  return Integer.parseInt(value);
		} catch (NumberFormatException ex) {
			return 0;
		}
	}
	
	private void saveSettings() {
		FileOutputStream fout = null;
		String filename = "events.settings";
		Properties properties = new Properties();
		properties.put("currentHour", Integer.toString(currentHour));
		properties.put("maxHours", Integer.toString(maxHours));
		properties.put("oldestHour", Integer.toString(oldestHour));
		properties.put("currentDay", Integer.toString(currentDay));
		properties.put("maxDays", Integer.toString(maxDays));
		properties.put("oldestDay", Integer.toString(oldestDay));
		properties.put("currentWeek", Integer.toString(currentWeek));
		properties.put("oldestWeek", Integer.toString(oldestWeek));
		properties.put("maxWeeks", Integer.toString(maxWeeks));
		properties.put("currentMonth", Integer.toString(currentMonth));
		properties.put("maxMonths", Integer.toString(maxMonths));
		properties.put("oldestMonth", Integer.toString(oldestMonth));
		
		try {
			fout = new FileOutputStream(filename);
			properties.store(fout, null);
		} catch (IOException ex) {
		    log.log(Level.WARNING, "error saving events.settings", ex);
		} finally {
			 if (fout != null) {
				 try {
					 fout.close();
				 } catch (IOException ex2) {}
			}
		}

	}
	
	private int getCurrentPeriodicity(int periodicityType) {
		int currentPeriodicity = currentHour;
		if (periodicityType == PERIOD_DAY)
			currentPeriodicity = currentDay;
		else if (periodicityType == PERIOD_WEEK)
			currentPeriodicity = currentWeek;
		else if (periodicityType == PERIOD_MONTH)
			currentPeriodicity = currentMonth;
		return currentPeriodicity;
	}
	
	// caching the prepared statements for optimal insert performance
	private PreparedStatement hourInsertPs = null, dayInsertPs = null, weekInsertPs = null, monthInsertPs = null;
	private static String templateTableName = "event_P_X";
	private String insertTemplate = "insert into " + templateTableName
			+ " (source_api_key, source_remote_ip, tenant_id, actor_uuid ,type, properties, "
			+ "dimensions, value, occurrence_time, ingestion_time, bucket) values "
			+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	private void insert(Event event) {

		Session sess = conn.getSession();
		String properties = event.getProperties();
		JSONObject obj = new JSONObject(properties);
		String value = obj.getString("URL");
		
		String sourceApiKey = event.getSourceApiKey();
		InetAddress sourceRemoteIp = event.getSourceRemoteIp();
		String tenantId = event.getTenantId();
		String actorUuid = event.getActorUuid();
		String type = event.getType();
		int dimensions = event.getDimensions();
		ZonedDateTime occurrenceTime = event.getOccurrenceTime();
		ZonedDateTime ingestionTime = event.getIngestionTime();
		int bucket  = occurrenceTime.getHour() * 10 + (occurrenceTime.getMinute() / 10);
		
		try {
			if (hourInsertPs == null) {
				String hourCql = insertTemplate.replace(templateTableName, "event_hour_" + currentHour);
				hourInsertPs = sess.prepare(hourCql);
			}
			BoundStatement hourBs = hourInsertPs.bind(sourceApiKey, sourceRemoteIp.getHostAddress(),
				tenantId, actorUuid, type, properties, dimensions, value, occurrenceTime, ingestionTime, bucket);
			sess.execute(hourBs);
		} catch (DriverException ex) {
			log.log(Level.WARNING, "at hour insert", ex);
		}
		try {
			if (dayInsertPs == null) {
				String dayCql = insertTemplate.replace(templateTableName, "event_day_" + currentDay);
				dayInsertPs = sess.prepare(dayCql);
			}
			BoundStatement dayBs = dayInsertPs.bind(sourceApiKey, sourceRemoteIp.getHostAddress(),
				tenantId, actorUuid, type, properties, dimensions, value, occurrenceTime, ingestionTime, bucket);
			sess.execute(dayBs);
		} catch (DriverException ex) {
			log.log(Level.WARNING, "at day insert", ex);
		}
		
		try {
			if (weekInsertPs == null) {
				String weekCql = insertTemplate.replace(templateTableName, "event_week_" + currentWeek);
				weekInsertPs = sess.prepare(weekCql);
			}
			BoundStatement weekBs = weekInsertPs.bind(sourceApiKey, sourceRemoteIp.getHostAddress(),
				tenantId, actorUuid, type, properties, dimensions, value, occurrenceTime, ingestionTime, bucket);
			sess.execute(weekBs);
		} catch (DriverException ex) {
			log.log(Level.WARNING, "at week insert", ex);
		}
		
		try {
			if (monthInsertPs == null) {
				String monthCql = insertTemplate.replace(templateTableName, "event_month_" + currentMonth);
				monthInsertPs = sess.prepare(monthCql);
			}
			BoundStatement monthBs = monthInsertPs.bind(sourceApiKey, sourceRemoteIp.getHostAddress(),
				tenantId, actorUuid, type, properties, dimensions, value, occurrenceTime, ingestionTime, bucket);
			sess.execute(monthBs);
		} catch (DriverException ex) {
			log.log(Level.WARNING, "at month insert", ex);
		}
	}
	
	private String makeTableName(int periodicityType, int bucketNo) {
		String typeString = "Hour";
		if (periodicityType == PERIOD_DAY)
			typeString = "Day";
		else if (periodicityType == PERIOD_WEEK)
			typeString = "Week";
		else if (periodicityType == PERIOD_MONTH)
			typeString = "Month";
		return "event_" + typeString + "_" + bucketNo;
	}

	private void createTable(int periodicityType, int bucketNo) {
		
		String tableName = makeTableName(periodicityType, bucketNo);
		String createStmt =
		"create table " + tableName + " ("
		+"	    source_api_key text,"
		+"	    source_remote_ip inet,"
		+"	    tenant_id text,"
		+"	    actor_uuid uuid,"
		+"	    type text,"
		+"	    properties frozen map,"
		+"	    dimensions int,"
		+"	    value text,"
		+"	    occurrence_time timestamp,"
		+"		ingestion_time timestamp,"
		+"      bucket int,"
		+"	    primary key((type, tenant_id, bucket), value, actor_uuid, occurrence_time)"
		+"	) WITH COMPACTION = {'class': 'TimeWindowCompactionStrategy', "
		+"	                     'compaction_window_unit': 'DAYS',"
		+"	                     'compaction_window_size': 1};";
		try {
			Session sess = conn.getSession();
			sess.execute(createStmt);
			String indexStmt = "create index " + tableName + "_alt on " + tableName + " ("
				+ "type, tenant_id, value, actor_uuid);";
			sess.execute(indexStmt);
		} catch (DriverException ex) {
			log.log(Level.WARNING, "create table or index", ex);
		}
	}

   // roll the numbers used to name the tables, avoiding resetting to 0 at start of year
    private void setupTimeSeriesRoll() {
	   
	  ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	 
      Runnable roller = new Runnable() {
	    public void run() {
		   currentHour++;
		   hourInsertPs = null;
		   createTable(PERIOD_HOUR, currentHour);
		   oldestHour = checkForRolloff(PERIOD_HOUR, currentHour, oldestHour, maxHours);
		   LocalDateTime date = LocalDateTime.now();
		   int newDay = date.getDayOfMonth();
		   if (newDay != dayValue) {
			   currentDay++;
			   dayValue = newDay;
			   dayInsertPs = null;
			   createTable(PERIOD_DAY, currentDay);
			   oldestDay = checkForRolloff(PERIOD_DAY, currentDay, oldestDay, maxDays);
		   }
		   TemporalField woy = WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear(); 
		   int newWeek = date.get(woy);
		   if (newWeek != weekValue) {
			   currentWeek++;
			   weekValue = newWeek;
			   weekInsertPs = null;
			   createTable(PERIOD_WEEK, currentWeek);
			   oldestWeek = checkForRolloff(PERIOD_WEEK, currentWeek, oldestWeek, maxWeeks);
		   }
		   int newMonth = date.getMonthValue();
		   if (newMonth != monthValue) {
			   currentMonth++;
			   monthValue = newMonth;
			   monthInsertPs = null;
			   createTable(PERIOD_MONTH, currentMonth);
			   oldestMonth = checkForRolloff(PERIOD_MONTH, currentMonth, oldestMonth, maxMonths);
		   }
	     }
       };
 
       long nextHour = LocalDateTime.now().withMinute(0).plusHours(1).toInstant(ZoneOffset.ofHours(0)).toEpochMilli();
       scheduler.scheduleAtFixedRate(roller, nextHour, 1,  TimeUnit.HOURS);
   	}
    
    private int checkForRolloff(int periodicityType, int current, int oldest, int max) {
    	
    	int count = oldest - current + 1;
    	while (count > max) {
    		dropTable(periodicityType, oldest);
    		oldest++;
    		count--;
    	}
    	return oldest;
    }
    
    private void dropTable(int periodicityType, int bucketNo) {
    	
    	String cql = "drop table " + makeTableName(periodicityType, bucketNo) + ";";
    	Session sess = conn.getSession();
    	try {
    		sess.execute(cql);
    	} catch (DriverException ex) {
    		log.log(Level.WARNING, "drop table", ex);
    	}
    }
    
    public void shutdown() {
    	conn.close();
    	saveSettings();
    }


	 public boolean isRunning() {
		 return running;
	 }
	
	class Processor extends Thread {
		
		volatile boolean waiting = false;

		public void run() {
			
			do {
				try {
					waiting = false;
					Event event = queue.getFirst();
					insert(event);
				} catch (NoSuchElementException ex) {
					// to be cooperative on cpu usage if there is a lull and not spin
					waiting = true;
					synchronized (processor) {
						try {
							processor.wait(30000);
						} catch (InterruptedException e) {
						}
					}
				}
			} while (running);
		}
	}
	
	/**
	 * 
	 * @param args: cassandra datacenter and contact point (address or host)
	 */
	public static final void main(String[] args) {
		UniqueEventTracker uet = null;
		try {
			uet = new UniqueEventTracker(args[0], args[1]);
			while (uet.isRunning()) {
				try {
					Thread.sleep(30000);
				} catch (InterruptedException e) {
				}
			}
		} finally {
			if (uet != null) {
				uet.shutdown();
			}
		}
	}
}
