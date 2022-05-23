package io.confluent.connect.jdbc.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.FilemakerDialect;
import io.confluent.connect.jdbc.source.TableQuerier.QueryMode;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnDefinition.Nullability;
import io.confluent.connect.jdbc.util.ColumnId;

@Category(IntegrationTest.class)
public class FilemakerDialectIT extends FilemakerDialectITBase {

	private static Logger log = LoggerFactory.getLogger(FilemakerDialectIT.class);

	private static final String FM_JDBC_CONNECT_PROPERTIES = "FilemakerJdbcConnect.properties";
	
	public Properties jdbcConnectionProperties() {
		Properties jdbcConnectionProperties = new Properties();
		try {
			jdbcConnectionProperties.load(FilemakerDialectIT.class.getClassLoader().getResourceAsStream(FM_JDBC_CONNECT_PROPERTIES));
			log.info(FM_JDBC_CONNECT_PROPERTIES + "loaded");
			return jdbcConnectionProperties;
			
		} catch (IOException e) {
			log.warn(FM_JDBC_CONNECT_PROPERTIES + " missing, skipping test execution");
			return null; 
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
	private static final String COLUMN_ROWID = "ROWID";
	private static final String COLUMN_ROWMODID = "ROWMODID";
	private static final String COLUMN_AENDERUNG_DATUM = "Aenderung_Datum";

	
	@Test
	public void testDescribeColumns_nullability() throws Exception {
		
		ColumnDefinition columnMetadata = columnMetadata(TABLE_OBJEKT, COLUMN_OBJEKT_ID);
		assertEquals(Nullability.NOT_NULL, columnMetadata.nullability());
		columnMetadata = columnMetadata(TABLE_OBJEKT, COLUMN_AENDERUNG_DATUM);	
		assertEquals(Nullability.NOT_NULL, columnMetadata.nullability());
		
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
	
	@Test
	@Ignore // fails with [FileMaker][FileMaker JDBC] Cursor has been closed.
	public void test_recordMedatada() throws SQLException {
		TimestampIncrementingTableQuerier tableQuerier = new TimestampIncrementingTableQuerier(
				(DatabaseDialect)fmDialect,
				QueryMode.TABLE,
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


}
