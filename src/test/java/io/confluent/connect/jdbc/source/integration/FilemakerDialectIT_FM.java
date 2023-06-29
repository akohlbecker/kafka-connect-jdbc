package io.confluent.connect.jdbc.source.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.FilemakerDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.TimestampIncrementingTableQuerier;
import io.confluent.connect.jdbc.source.TimestampIncrementingTableQuerierFactory;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnDefinition.Nullability;
import io.confluent.connect.jdbc.util.ColumnId;

/** 
 * Testing the Filemaker SQL Dialect implementation
 * <p>
 * Class name suffix "IT_FM" to put FileMaker tests into another suite which is not run by default.
 * <p>
 * There is no way to use FileMaker Server for testing without purchasing a license or applying for a 
 * test license at the customer service. So running integration tests which require a these FileMaker Server 
 * can not be fully automated.
 */ 
@Category(IntegrationTest.class)
public class FilemakerDialectIT_FM extends FilemakerDialectITBase {

	private static Logger log = LoggerFactory.getLogger(FilemakerDialectIT_FM.class);
	
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
	public void testCurrentTimeOnDB() throws Exception {
		fmDialect.currentTimeOnDB(fmDialect.getConnection(), Calendar.getInstance());
	}
	
	@Test
	public void testIsConnectionValid() throws Exception {
		// 5 seconds as in CachedConnectionProvider.VALIDITY_CHECK_TIMEOUT_S = 5
		fmDialect.isConnectionValid(fmDialect.getConnection(), 5);
	}

	private ColumnDefinition columnMetadataFromTableMetadata(String tableName, String columnName) throws SQLException {
		Map<ColumnId, ColumnDefinition> columnMetadataMap = fmDialect.describeColumns(fmDialect.getConnection(), tableName, columnName);
		assertTrue("No column returned, please  check the field name", columnMetadataMap.size() > 0);
		assertEquals("More than one columns returned, please  check the field name", 1, columnMetadataMap.size());
		assertEquals("\""+tableName+"\".\""+columnName+"\"", columnMetadataMap.keySet().iterator().next().toString());
		ColumnDefinition columnMetadata = columnMetadataMap.values().iterator().next();
		return columnMetadata;
	}

	
	private ColumnDefinition columnMetadataFromResultSet(String tableName, String columnName) throws SQLException {
		ResultSet rs = fetchFistRow(TABLE_OBJEKT);
		Map<String, ColumnDefinition> columnMetadataMap = fmDialect.describeColumns(rs.getMetaData()).entrySet()
				.stream()
				.collect(Collectors.toMap((e) -> e.getKey().toString(), Entry::getValue));
		assertTrue("No column returned, please  check the field name", columnMetadataMap.size() > 0);
		String key = "\"\".\""+columnName+"\"";
		assertTrue(columnMetadataMap.containsKey(key));
		ColumnDefinition columnMetadata = columnMetadataMap.get(key);
		return columnMetadata;
	}
	
	private ResultSet fetchFistRow(String tableName) throws SQLException {
		Statement statement = fmDialect.getConnection().createStatement();
        statement.execute("SELECT * FROM " + tableName  + " FETCH FIRST 1 ROWS ONLY ");
	      ResultSet rs = null;
	      try {
	        rs = statement.getResultSet();
	        return rs;
	      } finally {
	        if (rs != null) {
	          rs.close();
	        }
	      }
	}

	private static final long POLLING_INTERVAL_MS = 500;

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
	private static final String COLUMN_ROWID = "ROWID";
	private static final String COLUMN_AENDERUNG_DATUM = "Aenderung_Datum";
	private static final String COLUMN_ERSTELLUNG_DATUM = "Erstellung_Datum";
	private static final String OBJEKT_SIGNATUR_ARCHIV = "Objekt_Signatur_Archiv";

	
	@Test
	public void testDescribeColumns_fromTableMetadata_nullability() throws Exception {
		
		ColumnDefinition columnMetadata = columnMetadataFromTableMetadata(TABLE_OBJEKT, COLUMN_OBJEKT_ID);
		assertEquals(Nullability.NOT_NULL, columnMetadata.nullability());
		columnMetadata = columnMetadataFromTableMetadata(TABLE_OBJEKT, COLUMN_AENDERUNG_DATUM);	
		assertEquals(Nullability.NULL, columnMetadata.nullability());
	}
	
	@Test
	public void testDescribeColumns_fromTableMetadata_type_timestamp() throws Exception {
		ColumnDefinition columnMetadata = columnMetadataFromTableMetadata(TABLE_OBJEKT, COLUMN_AENDERUNG_DATUM);	
		assertEquals(Types.TIMESTAMP, columnMetadata.type());
	}
	
	
	@Test
	public void testDescribeColumns_fromTableMetadata_type_date() throws Exception {
		ColumnDefinition columnMetadata = columnMetadataFromTableMetadata(TABLE_OBJEKT, COLUMN_ERSTELLUNG_DATUM);
		assertEquals(Types.VARCHAR, columnMetadata.type());
		assertEquals("varchar", columnMetadata.typeName());
		assertNull(columnMetadata.classNameForType());
	}
	
	@Test
	public void testDescribeColumns_fromResultSet_type_date() throws Exception {
		ColumnDefinition columnMetadata = columnMetadataFromResultSet(TABLE_OBJEKT, COLUMN_ERSTELLUNG_DATUM);
		assertEquals(Types.VARCHAR, columnMetadata.type());
		assertEquals("varchar", columnMetadata.typeName());
		assertEquals("java.lang.String", columnMetadata.classNameForType());
	}
	
	@Test
	public void testDescribeColumns_fromResultSet_type_text() throws Exception {
		ColumnDefinition columnMetadata = columnMetadataFromResultSet(TABLE_OBJEKT, OBJEKT_SIGNATUR_ARCHIV);
		assertEquals(Types.VARCHAR, columnMetadata.type());
		assertEquals("varchar", columnMetadata.typeName());
		assertEquals("java.lang.String", columnMetadata.classNameForType());
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
		
		ColumnDefinition columnMetadata = columnMetadataFromTableMetadata(TABLE_OBJEKT, COLUMN_OBJEKT_ID);	
		assertTrue(columnMetadata.isAutoIncrement());
	}
	
	@Test
	public void testDescribeColumns_constraint_unique() throws Exception {
		
		ColumnDefinition columnMetadata = columnMetadataFromTableMetadata(TABLE_OBJEKT, COLUMN_OBJEKT_ID);	
		// assertTrue(columnMetadata.isAutoIncrement());
	}
	
	@Test
	@Ignore // fails with [FileMaker][FileMaker JDBC] Cursor has been closed.
	public void test_recordMedatada() throws SQLException {
		TimestampIncrementingTableQuerier tableQuerier = TimestampIncrementingTableQuerierFactory.newTimestampIncrementingTableQuerierTableMode(
				(DatabaseDialect)fmDialect,
				TABLE_OBJEKT,
				"topic-prefix",
				Arrays.asList(COLUMN_AENDERUNG_DATUM),
				COLUMN_ROWID,
				null,
				100l, // delay 100ms
				TimeZone.getDefault(),
				"", // suffix to be appended to the sql query string
				JdbcSourceConnectorConfig.TimestampGranularity.CONNECT_LOGICAL
				);
		tableQuerier.maybeStartQuery(fmDialect.getConnection());
		tableQuerier.extractRecord();
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
