package org.dhenry.samples.main;


import java.util.logging.Level;
import java.util.logging.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

/**
 * Class used for connecting to Cassandra database.
 */
public class CassandraConnection {

   private static final Logger log = Logger.getLogger(CassandraConnection.class.getName());
   private static final int REMOTE_USED_HOSTS = 2;
   
   private Cluster cluster;
   private Session session;
   
   /**
    * Connect to Cassandra Cluster and prefer nodes in local datacenter.
    *
    * @param localDataCenter Local datacenter name;
    * @param contactPoint 127.0.0.1 or another node IP address.
    */
   public void connect(final String localDataCenter, String contactPoint)
   {
	   this.cluster = Cluster.builder()
		        .addContactPoint(contactPoint)
		        .withLoadBalancingPolicy(
		                DCAwareRoundRobinPolicy.builder()
		                        .withLocalDc(localDataCenter)
		                        .withUsedHostsPerRemoteDc(REMOTE_USED_HOSTS)
		                        .allowRemoteDCsForLocalConsistencyLevel()
		                        .build()
		        ).build();

      final Metadata metadata = cluster.getMetadata();
      if (log.isLoggable(Level.FINE)) {
          log.fine("Connected to cluster: " + metadata.getClusterName());
          
	      for (Host host : metadata.getAllHosts())
	      {
	         log.fine(String.format("Datacenter: %s; Host: %s; Rack: %s",
	            host.getDatacenter(), host.getAddress(), host.getRack()));
	      }
      }
      session = cluster.connect();
   }
   /**
    * @return My session.
    */
   public Session getSession() {
      return this.session;
   }
   
   public String getReleaseVersion() {
     ResultSet rs = session.execute("select release_version from system.local");
	 Row row = rs.one();
	 String releaseVersion = row.getString("release_version");
	 if (log.isLoggable(Level.FINE))
	   log.fine("Cassandra Release Version: " + releaseVersion);
     return releaseVersion;
   }

   /** Connection must be closed. */
   public void close() {
	  try {
        cluster.close();
      } catch (Exception e) { }
   }
}