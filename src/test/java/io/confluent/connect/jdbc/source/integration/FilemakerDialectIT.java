package io.confluent.connect.jdbc.source.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.dialect.FilemakerDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnDefinition.Nullability;
import scala.reflect.internal.Trees.New;
import io.confluent.connect.jdbc.util.ColumnId;

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
			String jdbcURL =  FM_REMOTE_DB_URL + urlProps.getProperty(HOST_PORT_PATH_PARAMS);
			urlProps.put("SocketTimeout", "1000");
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

	private ColumnDefinition columnMetadata(String tableName, String columnName) throws SQLException {
		Map<ColumnId, ColumnDefinition> columnMetadataMap = fmDialect.describeColumns(fmDialect.getConnection(), tableName, columnName);
		assertEquals(1, columnMetadataMap.size());
		assertEquals("\""+tableName+"\".\""+columnName+"\"", columnMetadataMap.keySet().iterator().next().toString());
		ColumnDefinition columnMetadata = columnMetadataMap.values().iterator().next();
		return columnMetadata;
	}

	/* ***********************************************************************************************
	 * Database specific tests
	 * ***********************************************************************************************
	 * 
	 * Database: ZKM_Archiv
	 * -----------------------------
	 * Objekt.Aenderung_Datum:
	 *  - type: timestamp
	 *  - not null
	 *  - indexed
	 * Objekt.Objekt_ID:
	 *  - type: timestamp
	 *  - not null
	 *  - indexed
	 *  - autoincrement
	 *  - unique
     */
	private static final String TABLE_OBJEKT = "Objekt";
	private static final String COLUMN_OBJEKT_ID = "Objekt_ID";
	private static final String COLUMN_AENDERUNG_DATUM = "Aenderung_Datum";

	
	@Test
	public void testDescribeColumns_nullability() throws Exception {
		
		ColumnDefinition columnMetadata = columnMetadata(TABLE_OBJEKT, COLUMN_OBJEKT_ID);
		assertEquals(Nullability.NOT_NULL, columnMetadata.nullability());
		columnMetadata = columnMetadata(TABLE_OBJEKT, COLUMN_AENDERUNG_DATUM);	
		assertEquals(Nullability.NOT_NULL, columnMetadata.nullability());
		
	}
	
	/**
	 * see https://git.zkm.de/data-infrastructure/kafka-connect-jdbc-filemaker/-/issues/1
	 */
	@Test
	public void testDescribeColumns_type_numeric() throws Exception {
		
		ColumnDefinition columnMetadata = columnMetadata(TABLE_OBJEKT, COLUMN_OBJEKT_ID);	
		assertEquals(Types.INTEGER, columnMetadata.type());
	}
	
	@Test
	public void testDescribeColumns_type_timestamp() throws Exception {
		
		ColumnDefinition columnMetadata = columnMetadata(TABLE_OBJEKT, COLUMN_AENDERUNG_DATUM);	
		assertEquals(Types.TIMESTAMP, columnMetadata.type());
	}
	
//	@Test
//	public void testDescribeColumns_searchable() throws Exception {
//		
//		ColumnDefinition columnMetadata = columnMetadata(TABLE_OBJEKT, COLUMN_OBJEKT_ID);
//		assertTrue(columnMetadata.isSearchable());
//	}
	
	@Test
	@Ignore
	public void testDescribeColumns_autoincrement() throws Exception {
		
		ColumnDefinition columnMetadata = columnMetadata(TABLE_OBJEKT, COLUMN_OBJEKT_ID);	
		assertTrue(columnMetadata.isAutoIncrement());
	}
	
	@Test
	public void testDescribeColumns_constraint_unique() throws Exception {
		
		ColumnDefinition columnMetadata = columnMetadata(TABLE_OBJEKT, COLUMN_OBJEKT_ID);	
		// assertTrue(columnMetadata.isAutoIncrement());
	}


}
