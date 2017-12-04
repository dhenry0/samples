package org.dhenry.samples.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.*;
import org.dhenry.samples.main.CassandraConnection;

public class CassandraConnectionTests {

    @Test
    public void connectionToCassandraShouldSucceed() {
        CassandraConnection conn = null;
        try {
        	conn = new CassandraConnection();
        	
        	assertNotNull(conn);
            assertEquals("3.1.2", conn.getReleaseVersion(), "current release version must be 3.1.2");

        } finally {
          if (conn != null) conn.close();
        }
        
    }
}