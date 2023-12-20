package io.confluent.connect.jdbc.source.integration;

import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.AbstractStatus.State;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.dialect.FilemakerDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;

/** 
 * Class name suffix "IT_FM" to put FileMaker tests into another suite which is not run by default.
 * <p>
 * There is no way to use FileMaker Server for testing without purchasing a license or applying for a 
 * test license at the customer service. So running integration tests which require a these FileMaker Server 
 * can not be fully automated.
 */ 
@Category(IntegrationTest.class)
public class FilemakerDialectConnectIT_FM extends FilemakerDialectITBase {


	private static final String BATCH_MAX_ROWS = "100";
	private static final String TOPIC_PREFIX = "topic_";
	private static Logger logger = LoggerFactory.getLogger(FilemakerDialectConnectIT_FM.class);
	private static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);

	private static final long POLLING_INTERVAL_MS =  TimeUnit.SECONDS.toMillis(10); //  5 m = 300000 ms;
	private static final int POLLING_ITERATIONS = 4; // 50
	
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
	
	Map<String, String> jdbcSourceConnectorProps_1;
	Map<String, String> jdbcSourceConnectorProps_2;
    Map<String, String> jdbcSourceConnectorProps_3;
  
    Admin adminClient;
	private KafkaConsumer<String, String> consumerClient;
	
	private Map<String, String> jdbcFMSourceConfiguration(String tableName, boolean usePerHostConnectionPool) {
		
		Map<String, String> props =  new HashMap<>();
	  	props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, JdbcSourceConnector.class.getName());
	  	props.put(TASKS_MAX_CONFIG, "1");
	  	props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, composeJdbcUrlWithAuth(jdbcURL_db1, db1_UserAccountQueryParamsString));
	  	props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, "id");
	  	
	  	// when using Bulk mode the BulkTableQuerier always queries for the full table: SELECT * FROM {TABLE}
	  	// but JdbcSourceTask.poll() will only consume batch.max.rows and send them to the topic.
	  	// setting batch.max.rows to a high value helps reducing the poll frequency in order to get all 
	  	// records 
	  	props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
	  	props.put(JdbcSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG, BATCH_MAX_ROWS);
	  	props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, Long.toString(POLLING_INTERVAL_MS));
	  	props.put(JdbcSourceConnectorConfig.TABLE_POLL_INTERVAL_MS_CONFIG, Long.toString(TimeUnit.DAYS.toMillis(1)));
	  	
	  	props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
	  	props.put(JdbcSourceConnectorConfig.VALIDATE_NON_NULL_CONFIG, "false");
	  	
	  	props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, tableName);
	  	
	  	return props;
	}

	@Before
	public void before() throws InterruptedException, ExecutionException {
		
		jdbcSourceConnectorProps_1 = jdbcFMSourceConfiguration("Archive_zu_Dokument", false);
		jdbcSourceConnectorProps_2 = jdbcFMSourceConfiguration("Bestand", false);
		jdbcSourceConnectorProps_3 = jdbcFMSourceConfiguration("Objekt", false);

		// start the clusters
		logger.debug("Starting embedded Connect worker, Kafka broker, and ZK");
		startConnect();
		
		// create topic in Kafka
		connect.kafka().createTopic(TOPIC_PREFIX + "Archive_zu_Dokument");
		connect.kafka().createTopic(TOPIC_PREFIX + "Bestand");
		connect.kafka().createTopic(TOPIC_PREFIX + "Objekt");

		adminClient = connect.kafka().createAdminClient();
		consumerClient = createConsumerClient();
	}

	@After
	public void after() {
		adminClient.close();
		consumerClient.close();
		stopConnect();
	}
	
	/**
	 * Create a {@link JdbcSourceConnectorConfig} with the specified URL and
	 * optional config props.
	 *
	 * @param url the database URL; may not be null
	 * @return the config; never null
	 */
	protected JdbcSourceConnectorConfig sourceConfigWithUrl(String url, String... propertyPairs) {
		Map<String, String> connProps = new HashMap<>();
		connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
		connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
		// connProps.putAll(propertiesFromPairs(propertyPairs));
		connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, url);
		return new JdbcSourceConnectorConfig(connProps);
	}
  
	// @Test // The hikari connection pool implementation has been rejected and should be removed
	public void testConnectorsParallelStart_withPool() throws Exception {
		connect.configureConnector(CONNECTOR_NAME_1, jdbcFMSourceConfiguration("Archive_zu_Dokument", true));
		connect.configureConnector(CONNECTOR_NAME_2, jdbcFMSourceConfiguration("Bestand", true));
		connect.configureConnector(CONNECTOR_NAME_3, jdbcFMSourceConfiguration("Objekt",  true));
		waitForConnectorToStart(CONNECTOR_NAME_1, 1);
		waitForConnectorToStart(CONNECTOR_NAME_2, 1);
		// waitForConnectorToStart(CONNECTOR_NAME_3, 1);
		logger.info(">>>>>>>>>>>>>>>>>>>>>> all connectors started");
		logger.info(".... polling for " + (POLLING_INTERVAL_MS * POLLING_ITERATIONS / 1000) + " s ....");
		Thread.sleep(POLLING_INTERVAL_MS * POLLING_ITERATIONS);
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
	
	/**
	 * Test without <code>POLL_SLEEP_MS_CONFIG</code>
	 */
	@Test
	public void test_polling_no_pause() throws Exception {
	
		long recordsInDB = 248;
		// receive at least twice the time records as are in the source table
		int pollingIterations = Double.valueOf(Math.ceil(recordsInDB / Double.valueOf(BATCH_MAX_ROWS))).intValue() * 2;
		long totalPollingTime = POLLING_INTERVAL_MS * pollingIterations;
		
		String topicName = TOPIC_PREFIX + "Bestand";

		connect.resetConnectorTopics(CONNECTOR_NAME_2);
		assertEquals("Topic should be empty after reset",  0, countMessages(topicName));
		
		Map<String, String> connectorConf = jdbcFMSourceConfiguration("Bestand", false);
		connect.configureConnector(CONNECTOR_NAME_2, connectorConf);
		waitForConnectorToStart(CONNECTOR_NAME_2, 1);
		logger.info(">>>>>>>>>>>>>>>>>>>>>> all connectors started");
		logger.info(".... polling for " + (totalPollingTime / 1000) + " s ....");
		Thread.sleep(totalPollingTime);
		logger.info(">>>>>>>>>>>>>>>>>>>>>> ending test after waiting for " + pollingIterations + " poll intervals");
		connect.pauseConnector(CONNECTOR_NAME_2);
		
		long numberOfMessages = countMessages(topicName);
		
		logger.debug("number of messages in {} : {} ", topicName, numberOfMessages);
		assertTrue("Expecting more messages than records in the table", numberOfMessages > recordsInDB);
	}
	
	/**
	 * Test with <code>POLL_SLEEP_MS_CONFIG</code>
	 */
	@Test
	public void test_polling_with_pause() throws Exception {
	
		long recordsInDB = 248;
		// receive at least twice the time records as are in the source table
		int pollingIterations = Double.valueOf(Math.ceil(recordsInDB / Double.valueOf(BATCH_MAX_ROWS))).intValue() * 2;
		long totalPollingTime = POLLING_INTERVAL_MS * pollingIterations;
		
		String topicName = TOPIC_PREFIX + "Bestand";

		connect.resetConnectorTopics(CONNECTOR_NAME_2);
		assertEquals("Topic should be empty after reset",  0, countMessages(topicName));
		
		Map<String, String> connectorConf = jdbcFMSourceConfiguration("Bestand", false);
		// pause after polling for total polling time
		connectorConf.put(JdbcSourceConnectorConfig.POLL_SLEEP_MS_CONFIG, Long.toString(totalPollingTime));
		connect.configureConnector(CONNECTOR_NAME_2, connectorConf);
		
		
		waitForConnectorToStart(CONNECTOR_NAME_2, 1);
		logger.info(">>>>>>>>>>>>>>>>>>>>>> all connectors started");
		logger.info(".... polling for " + (totalPollingTime / 1000) + " s ....");
		Thread.sleep(totalPollingTime);
		logger.info(">>>>>>>>>>>>>>>>>>>>>> ending test after waiting for " + pollingIterations + " poll intervals");
		connect.pauseConnector(CONNECTOR_NAME_2);
		
		long numberOfMessages = countMessages(topicName);
		
		logger.debug("number of messages in {} : {} ", topicName, numberOfMessages);
		assertEquals("Expecting same number of messages as records in the table", numberOfMessages, recordsInDB);
	}

	protected long countMessages(String topicName) {
		List<TopicPartition> partitions = consumerClient.partitionsFor(topicName).stream().map(p -> new TopicPartition(topicName, p.partition()))
			    .collect(Collectors.toList());
		consumerClient.assign(partitions);
		consumerClient.seekToEnd(Collections.emptySet());
		Map<TopicPartition, Long> endPartitions = partitions.stream().collect(Collectors.toMap(Function.identity(), consumerClient::position));
		long numberOfMessages = partitions.stream().mapToLong(p -> endPartitions.get(p)).sum();
		return numberOfMessages;
	}
  
	@Test
	public void testConnectorsParallelStart_noPool() throws Exception {
	
		connect.configureConnector(CONNECTOR_NAME_1, jdbcSourceConnectorProps_1);
		connect.configureConnector(CONNECTOR_NAME_2, jdbcSourceConnectorProps_2);
//		connect.configureConnector(CONNECTOR_NAME_3, jdbcSourceConnectorProps_3);

		waitForConnectorToStart(CONNECTOR_NAME_1, 1);
		waitForConnectorToStart(CONNECTOR_NAME_2, 1);
		// waitForConnectorToStart(CONNECTOR_NAME_3, 1);
		logger.info(">>>>>>>>>>>>>>>>>>>>>> all connectors started");
		logger.info(".... polling for " + (POLLING_INTERVAL_MS * POLLING_ITERATIONS / 1000) + " s ....");
		Thread.sleep(POLLING_INTERVAL_MS * POLLING_ITERATIONS);
		logger.info(">>>>>>>>>>>>>>>>>>>>>> ending test after waiting for " + POLLING_ITERATIONS + " poll intervals");
		
		
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
        "Connector tasks did not transition to state " + state + " after " + timeoutMs + " ms"
    );
    return System.currentTimeMillis();
  }

	
	@Test
	public void testConcurrentConnections_1() throws SQLException, InterruptedException {
		
		List<FilemakerDialect> dialectInstances = new ArrayList<>();
		int numConnections = 5;
		for (int i = 0; i < numConnections; i++) {
			dialectInstances.add( 
					new FilemakerDialect(sourceConfigWithUrl(composeJdbcUrlWithAuth(jdbcURL_db1, db1_UserAccountQueryParamsString)))
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

}
