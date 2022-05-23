package io.confluent.connect.jdbc.source;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.jdbc.dialect.FilemakerDialect;

public abstract class FilemakerDialectITBase {
	
	private static Logger log = LoggerFactory.getLogger(FilemakerDialectITBase.class);

	
	protected static final String JDBC_SOCKET_TIMEOUT = "10000"; // 10s
	protected final static String FM_REMOTE_DB_URL = "jdbc:filemaker://";
	
	protected static final String HOST_PORT_PATH_PARAMS =  "host_port_path_params";
	protected static final String USER =  "user";
	protected static final String PASS =  "password";
	
	private Map<String, String> props;
	FilemakerDialect fmDialect;
	
	
	@Before
	public void before() {
		props = new HashMap<>();
		
			Properties jdbcConnectionProperties = jdbcConnectionProperties();
			log.info("-------------------------------------------------------");
			log.info("    " + HOST_PORT_PATH_PARAMS + ":" + jdbcConnectionProperties.getProperty(HOST_PORT_PATH_PARAMS));
			log.info("    " + USER + ":" + jdbcConnectionProperties.getProperty(USER));
			log.info("    " + PASS + ":" + jdbcConnectionProperties.getProperty(PASS));
			log.info("-------------------------------------------------------");
			String jdbcURL =  FM_REMOTE_DB_URL + jdbcConnectionProperties.getProperty(HOST_PORT_PATH_PARAMS);
			// urlProps.put("SocketTimeout", JDBC_SOCKET_TIMEOUT);
			props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, jdbcURL);
			props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, jdbcConnectionProperties.getProperty(USER));
			props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, jdbcConnectionProperties.getProperty(PASS));
			props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
			props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, "topic_");
			FilemakerDialect.setClientTimeoutSeconds(60 * 60 * 10);
			fmDialect = new FilemakerDialect(new JdbcSourceConnectorConfig(props));
			
		
	}
	
	public abstract Properties jdbcConnectionProperties();

}
