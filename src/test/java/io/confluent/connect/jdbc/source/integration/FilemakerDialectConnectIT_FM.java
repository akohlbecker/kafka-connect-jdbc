package io.confluent.connect.jdbc.source.integration;

import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.AbstractStatus.State;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.dialect.ConnectionPoolProvider;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.FilemakerDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.source.TimestampIncrementingTableQuerier;
import io.confluent.connect.jdbc.source.TimestampIncrementingTableQuerierFactory;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnDefinition.Nullability;
import io.confluent.connect.jdbc.util.ColumnId;

/** 
 * Class name suffix "IT_FM" to put FileMaker tests into another suite which is not run by default.
 * <p>
 * There is no way to use FileMaker Server for testing without purchasing a license or applying for a 
 * test license at the customer service. So running integration tests which require a these FileMaker Server 
 * can not be fully automated.
 */ 
@Category(IntegrationTest.class)
public class FilemakerDialectConnectIT_FM extends FilemakerDialectITBase {

	private static Logger logger = LoggerFactory.getLogger(FilemakerDialectConnectIT_FM.class);

	private static final String FM_JDBC_CONNECT_PROPERTIES = "FilemakerJdbcConnect.properties";
	private static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
	private static final long POLLING_INTERVAL_MS =  TimeUnit.MINUTES.toMillis(5); // 300000;
	
//	
//	@ClassRule
//  @SuppressWarnings("deprecation")
//  public static final FixedHostPortGenericContainer fmServer =
//          new FixedHostPortGenericContainer<>("filemakerServer19:latest")
//              .withFileSystemBind("src/test/resources/filemaker19_dbs/", "/opt/FileMaker/FileMaker Server/Data/Databases/")
//              .withFixedExposedPort(5003, 5003)
//              .withFixedExposedPort(2399, 2399)
//              .withFixedExposedPort(16000, 16000)
//              .withFixedExposedPort(443, 443)
//              .withFixedExposedPort(80, 80)
//              .withFixedExposedPort(16001, 16001);

	

	private static final String CONNECTOR_NAME_1 = "JdbcFMSourceConnector_1";
	private static final String CONNECTOR_NAME_2 = "JdbcFMSourceConnector_2";
	private static final String CONNECTOR_NAME_3 = "JdbcFMSourceConnector_3";
  EmbeddedConnectCluster connect;
  Map<String, String> jdbcSourceConnectorProps_1;
  Map<String, String> jdbcSourceConnectorProps_2;
  Map<String, String> jdbcSourceConnectorProps_3;
  
  @Before
  public void before() {
  	super.before();
  	jdbcSourceConnectorProps_1 = jdbcFMSourceConfiguration();
  	jdbcSourceConnectorProps_1.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "Archive_zu_Dokument");
  	jdbcSourceConnectorProps_2 = jdbcFMSourceConfiguration();
  	jdbcSourceConnectorProps_2.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "Archive");
  	jdbcSourceConnectorProps_3 = jdbcFMSourceConfiguration();
  	jdbcSourceConnectorProps_3.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "Objekt");

    connect = new EmbeddedConnectCluster.Builder()
        .name("connect-cluster")
        .numWorkers(1)
        .brokerProps(new Properties())
        .build();

    // start the clusters
    logger.debug("Starting embedded Connect worker, Kafka broker, and ZK");
    connect.start();
  }

	private Map<String, String> jdbcFMSourceConfiguration() {
		Map<String, String> props = new HashMap<>();
  	props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, JdbcSourceConnector.class.getName());
  	props.put(TASKS_MAX_CONFIG, "1");
  	props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, composeJdbcUrlWithAuth(jdbcURL_db1));
  	props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
  	props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, "id");
  	props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, Long.toString(POLLING_INTERVAL_MS));
  	props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, "topic_");
  	props.put(JdbcSourceConnectorConfig.VALIDATE_NON_NULL_CONFIG, "false");
  	
  	props.put(JdbcSourceConnectorConfig.CONNECTION_POOL_CONFIG, "true");
  	
  	return props;
	}

  @After
  public void after() {
    if (connect != null) {
      connect.stop();
    }
  }

	
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
	
	private Properties jdbcConnectionPoolProperties() {
		Properties props =  jdbcConnectionProperties(); 
		props.put(JdbcSourceConnectorConfig.CONNECTION_POOL_CONFIG, true);
		return props;
	}
	
	/**
   * Create a {@link JdbcSourceConnectorConfig} with the specified URL and optional config props.
   *
   * @param url           the database URL; may not be null
   * @return the config; never null
   */
  protected JdbcSourceConnectorConfig sourceConfigWithUrl(
      String url,
      String... propertyPairs
  ) {
    Map<String, String> connProps = new HashMap<>();
    connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
    // connProps.putAll(propertiesFromPairs(propertyPairs));
    connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, url);
    return new JdbcSourceConnectorConfig(connProps);
  }
  
  @Test
  public void testConnectorsParallelStart() throws Exception {
 
    connect.configureConnector(CONNECTOR_NAME_1, jdbcSourceConnectorProps_1);
    connect.configureConnector(CONNECTOR_NAME_2, jdbcSourceConnectorProps_2);
    connect.configureConnector(CONNECTOR_NAME_3, jdbcSourceConnectorProps_3);

    waitForConnectorToStart(CONNECTOR_NAME_1, 1);
    waitForConnectorToStart(CONNECTOR_NAME_2, 1);
    waitForConnectorToStart(CONNECTOR_NAME_3, 1);
    logger.info(">>>>>>>>>>>>>>>>>>>>>> all connectors started");
    Thread.sleep(POLLING_INTERVAL_MS * 50);
    logger.info(">>>>>>>>>>>>>>>>>>>>>> ending test after waiting for 5 poll intervals");
//
//    connect.requestPut(connect.endpointForResource(String.format("connectors/%s/pause", CONNECTOR_NAME_1)), "");
//
//    waitForConnectorState(CONNECTOR_NAME_1, 1, 3*POLLING_INTERVAL_MS, State.PAUSED);
//
//    connect.requestPut(connect.endpointForResource(String.format("connectors/%s/resume", CONNECTOR_NAME_1)), "");
//    waitForConnectorState(CONNECTOR_NAME_1, 1,
//        3*POLLING_INTERVAL_MS, State.RUNNING);
//    
  }

  protected Optional<Boolean> assertConnectorAndTasksStatus(String connectorName, int numTasks, AbstractStatus.State expectedStatus) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
          && info.tasks().size() >= numTasks
          && info.connector().state().equals(expectedStatus.toString())
          && info.tasks().stream().allMatch(s -> s.state().equals(expectedStatus.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      logger.debug("Could not check connector state info.", e);
      return Optional.empty();
    }
  }

  protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    return waitForConnectorState(name, numTasks, CONNECTOR_STARTUP_DURATION_MS, State.RUNNING);
  }

  protected long waitForConnectorState(String name, int numTasks, long timeoutMs, State state) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksStatus(name, numTasks, state).orElse(false),
        timeoutMs,
        "Connector tasks did not transition to state " + state + " in time"
    );
    return System.currentTimeMillis();
  }

	
	@Test
	public void testConcurrentConnections_1() throws SQLException, InterruptedException {
		
		List<FilemakerDialect> dialectInstances = new ArrayList<>();
		int numConnections = 5;
		for (int i = 0; i < numConnections; i++) {
			dialectInstances.add( 
					new FilemakerDialect(sourceConfigWithUrl(jdbcURL_db1 + "?" + dbUserAccountQueryParamsString + "&SocketTimeout=" + JDBC_SOCKET_TIMEOUT))
			);
		}
		List<SQLException> exceptions = new ArrayList<>();
		
		dialectInstances.forEach( d -> {
			try {
				d.getConnection();
			} catch (SQLException e) {
				System.err.println(e.getMessage());
				exceptions.add(e);
			}
		});
		
		assertTrue(exceptions.isEmpty());
		
		Thread.sleep(POLLING_INTERVAL_MS);
		
		dialectInstances.forEach( d -> {
			try {
				d.getConnection();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		});
		
		assertTrue(exceptions.isEmpty());
		
		Thread.sleep(POLLING_INTERVAL_MS);
		
		dialectInstances.forEach( d -> {
			try {
				d.close();
				d.getConnection();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		});
		
		assertTrue(exceptions.isEmpty());
	}
	
	@Test
	public void testGetPool() throws SQLException {
		FilemakerDialect dialect_db1_1 = new FilemakerDialect(sourceConfigWithUrl(jdbcURL_db1 + "?" + dbUserAccountQueryParamsString + "&SocketTimeout=" + JDBC_SOCKET_TIMEOUT));
		FilemakerDialect dialect_db1_2 = new FilemakerDialect(sourceConfigWithUrl(jdbcURL_db1 + "?" + dbUserAccountQueryParamsString + "&SocketTimeout=" + JDBC_SOCKET_TIMEOUT));
		FilemakerDialect dialect_db2_1 = new FilemakerDialect(sourceConfigWithUrl(jdbcURL_db2 + "?" + dbUserAccountQueryParamsString + "&SocketTimeout=" + JDBC_SOCKET_TIMEOUT));

		ConnectionPoolProvider.singleton().getConnectionPool(jdbcConnectionPoolProperties(), dialect_db1_1);
		assertEquals(1, ConnectionPoolProvider.singleton().poolCount());
		ConnectionPoolProvider.singleton().getConnectionPool(jdbcConnectionPoolProperties(), dialect_db1_2);
		assertEquals(1, ConnectionPoolProvider.singleton().poolCount());
		ConnectionPoolProvider.singleton().getConnectionPool(jdbcConnectionPoolProperties(), dialect_db2_1);
		assertEquals(2, ConnectionPoolProvider.singleton().poolCount());
		ConnectionPoolProvider.singleton().release(dialect_db1_1);
		assertEquals(2, ConnectionPoolProvider.singleton().poolCount());
		ConnectionPoolProvider.singleton().release(dialect_db1_2);
		assertEquals(1, ConnectionPoolProvider.singleton().poolCount());
		ConnectionPoolProvider.singleton().release(dialect_db2_1);
		assertEquals(0, ConnectionPoolProvider.singleton().poolCount());
	}


}
