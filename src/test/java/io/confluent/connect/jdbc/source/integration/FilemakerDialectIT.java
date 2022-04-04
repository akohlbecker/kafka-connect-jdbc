package io.confluent.connect.jdbc.source.integration;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.dialect.FilemakerDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;

@Category(IntegrationTest.class)
public class FilemakerDialectIT {

	private static Logger log = LoggerFactory.getLogger(FilemakerDialectIT.class);

	private final static String FM_REMOTE_DB_URL = "jdbc:filemaker://";

	private static final String FM_JDBC_CONNECT_PROPERTIES = "FilemakerJdbcConnect.properties";
	private static final String HOST_PORT_PATH_PARAMS =  "host_port_path_params";
	private static final String USER =  "user";
	private static final String PASS =  "password";
	

	private Map<String, String> props;
	FilemakerDialect fmDialect;

	
	@Before
  public void before() {
		props = new HashMap<>();
		Properties urlProps = new Properties();
		try {
			urlProps.load(FilemakerDialectIT.class.getClassLoader().getResourceAsStream(FM_JDBC_CONNECT_PROPERTIES));
			log.info("-------------------------------------------------------");
			log.info(FM_JDBC_CONNECT_PROPERTIES + "loaded:");
			log.info("    " + HOST_PORT_PATH_PARAMS + ":" + urlProps.getProperty(HOST_PORT_PATH_PARAMS));
			log.info("    " + USER + ":" + urlProps.getProperty(USER));
			log.info("    " + PASS + ":" + urlProps.getProperty(PASS));
			log.info("-------------------------------------------------------");
			String jdbcURL =  FM_REMOTE_DB_URL + urlProps.getProperty(HOST_PORT_PATH_PARAMS) ;
			props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, jdbcURL);
			props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, urlProps.getProperty(USER));
			props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, urlProps.getProperty(PASS));
			props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
			props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, "topic_");
			fmDialect = new FilemakerDialect(new JdbcSourceConnectorConfig(props));
			
		} catch (IOException e) {
			log.warn(FM_JDBC_CONNECT_PROPERTIES + " missing, skipping test execution");
			return; 
		}
  }

	@Test
	public void testCurrentTimeOnDB() throws Exception {
		fmDialect.currentTimeOnDB(fmDialect.getConnection(), Calendar.getInstance());
	}
	
	@Test
	public void testIsConnectionValid() throws Exception {
		// 5 seconds as in CachedConnectionProvider.VALIDITY_CHECK_TIMEOUT_S = 5
		fmDialect.isConnectionValid(fmDialect.getConnection(), 5);
	}

}
