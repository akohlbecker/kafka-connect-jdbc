package io.confluent.connect.jdbc.source.integration;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.test.MockDeserializer;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.jdbc.dialect.FilemakerDialect;
import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;

public abstract class FilemakerDialectITBase extends BaseConnectorIT {
	
	private static Logger log = LoggerFactory.getLogger(FilemakerDialectITBase.class);
	
	private static final String FM_JDBC_CONNECT_PROPERTIES = "FilemakerJdbcConnect.properties";
	
	protected static final String JDBC_SOCKET_TIMEOUT = "10000"; // 10s
	protected final static String FM_REMOTE_DB_URL = "jdbc:filemaker://";
	
	protected static final String HOST_PORT_PATH_PARAMS_DB1 =  "host_port_path_params_db_1";
	protected static final String HOST_PORT_PATH_PARAMS_DB2 =  "host_port_path_params_db_2";
	protected static final String USER_DB1 =  "user_db_1";
	protected static final String USER_DB2 =  "user_db_2";
	protected static final String PASS_DB1 =  "password_db_1";
	protected static final String PASS_DB2 =  "password_db_2";
	
	private Map<String, String> props;
	FilemakerDialect fmDialect;

	protected String jdbcURL_db1;
	protected String jdbcURL_db2;
	protected String db1_UserAccountQueryParamsString;
	protected String db2_UserAccountQueryParamsString;
	
	private static Logger logger = LoggerFactory.getLogger(FilemakerDialectConnectIT_FM.class);
	

	public Properties jdbcConnectionProperties() {
		Properties jdbcConnectionProperties = new Properties();
		try {
			jdbcConnectionProperties.load(FilemakerDialectConnectIT_FM.class.getClassLoader().getResourceAsStream(FM_JDBC_CONNECT_PROPERTIES));
			logger.info(FM_JDBC_CONNECT_PROPERTIES + "loaded");
			return jdbcConnectionProperties;
			
		} catch (IOException e) {
			logger.warn(FM_JDBC_CONNECT_PROPERTIES + " missing, skipping test execution");
			return null; 
		}
	}
    
	@Before
	public void initFilemakerDialect() throws InterruptedException, ExecutionException {

		props = new HashMap<>();
		Properties jdbcConnectionProperties = jdbcConnectionProperties();
		log.info("-------------------------------------------------------");
		log.info("    " + HOST_PORT_PATH_PARAMS_DB1 + ":"
				+ jdbcConnectionProperties.getProperty(HOST_PORT_PATH_PARAMS_DB1));
		log.info("    " + USER_DB1 + ":" + jdbcConnectionProperties.getProperty(USER_DB1));
		log.info("    " + PASS_DB1 + ":" + jdbcConnectionProperties.getProperty(PASS_DB1));
		log.info("-------------------------------------------------------");
		jdbcURL_db1 = FM_REMOTE_DB_URL + jdbcConnectionProperties.getProperty(HOST_PORT_PATH_PARAMS_DB1);
		jdbcURL_db2 = FM_REMOTE_DB_URL + jdbcConnectionProperties.getProperty(HOST_PORT_PATH_PARAMS_DB2);
		db1_UserAccountQueryParamsString = "user=" + jdbcConnectionProperties.getProperty(USER_DB1) + "&password=" + jdbcConnectionProperties.getProperty(PASS_DB1);
		db2_UserAccountQueryParamsString = "user=" + jdbcConnectionProperties.getProperty(USER_DB2) + "&password=" + jdbcConnectionProperties.getProperty(PASS_DB2);
		// urlProps.put("SocketTimeout", JDBC_SOCKET_TIMEOUT);
		props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, jdbcURL_db1);
		props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, jdbcConnectionProperties.getProperty(USER_DB1));
		props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, jdbcConnectionProperties.getProperty(PASS_DB1));
		props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
		props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, "topic_");
		FilemakerDialect.setClientTimeoutSeconds(60 * 60 * 10);
		fmDialect = new FilemakerDialect(new JdbcSourceConnectorConfig(props));

	}
	
	protected String composeJdbcUrlWithAuth(String baseJdbcURL, String dbUserAccountQueryParamsString) {
		return baseJdbcURL + "?" + dbUserAccountQueryParamsString + "&SocketTimeout=" + JDBC_SOCKET_TIMEOUT;
	}
	
	public KafkaConsumer<String, String> createConsumerClient() {
		final Properties clientConfig = new Properties();
        clientConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
        clientConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MockDeserializer.class);
        clientConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MockDeserializer.class);
   
		KafkaConsumer<String,String> consumer = new KafkaConsumer<>(clientConfig);
		return consumer;
	}

	@After
	public void tearDown() throws SQLException {
		stopConnect();
	}
	
}
