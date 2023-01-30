package io.confluent.connect.jdbc.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnDefinition.Mutability;
import io.confluent.connect.jdbc.util.ColumnDefinition.Nullability;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;

/**
 * Complies to Filemaker JDBC v.16
 * 
 * @author a.kohlbecker
 */
public class FilemakerDialect extends GenericDatabaseDialect {

	private static final Logger logger = LoggerFactory.getLogger(FilemakerDialect.class);
	
	private static int CLIENT_TIMEOUT_SECONDS = 60;
	
	/**
	 * Factor to delay starting the connector.
	 */
	private static float CLIENT_START_JITTER = 0f;
	
	/**
	 * The ROWID system column contains the unique ID number of the record.
	 */
	private static final String COLUMN_ROWID = "ROWID";
	
	/**
	 * The ROWMODID system column contains the total number of times changes 
	 * to the current record have been committed. 
	 */
	private static final String COLUMN_ROWMODID = "ROWMODID";

	
	private static Integer clientTimeoutSeconds = null;

	public static Integer getClientTimeoutSeconds() {
		return clientTimeoutSeconds != null ? clientTimeoutSeconds : CLIENT_TIMEOUT_SECONDS;
	}
	
	public static void setClientTimeoutSeconds(Integer seconds) {
		clientTimeoutSeconds = seconds;
	}
	
	protected int startupDelay() {
		int delay = 0;
		if(CLIENT_START_JITTER > 0) {
			int pollInterval = config.getInt(JdbcSourceTaskConfig.POLL_INTERVAL_MS_CONFIG);
			int jitterRange = Math.round(CLIENT_START_JITTER * pollInterval);
			Random rand = new Random();
			delay = rand.nextInt(jitterRange);
			logger.debug("startupDelay: " + delay);
		}
		return delay;
	}
	
	/**
	 * The provider for {@link Filemaker16Dialect}.
	 */
	public static class Provider extends SubprotocolBasedProvider {

		public Provider() {
			super(FilemakerDialect.class.getSimpleName(), "filemaker");
		}

		@Override
		public DatabaseDialect create(AbstractConfig config) {
			return new FilemakerDialect(config);
		}
	}

	/**
	 * Using the system table FileMaker_Tables as reference table
	 * as this always exists even in empty dbs.
	 */
	private String REFERENCE_TABLE_NAME = "FileMaker_Tables";

	public FilemakerDialect(AbstractConfig config) {
		super(config);
	}

	public FilemakerDialect(AbstractConfig config, IdentifierRules defaultIdentifierRules) {
		super(config, defaultIdentifierRules);
	}
	
	/**
	 * Wrapper implementation around {@link GenericDatabaseDialect#getConnection()} to deal with 
	 * a Filemaker Server problem. The Filemaker Server can become unresponsive, but the 
	 * Filemaker JDBC driver is not detecting this situation properly. The main thread 
	 * can be blocked by this for very long time.
	 * <p>
	 * To prevent from blocking the main thread this implementation delegated the getConnection() call
	 * to a sub-thread for which this method here will wait for 60 seconds.
	 * 
	 */
	@Override
	public Connection getConnection() throws SQLException {
		
		final Connection[] connection = new Connection[] {null};
		final SQLException[] exceptions = new SQLException[] {null};

		final CountDownLatch latch = new CountDownLatch(1);
		Runnable getConnectionWorker = new Runnable(){
			@Override
	        public void run(){
//						try {
////							Thread.sleep(startupDelay());
//						} catch (InterruptedException e1) {
//							logger.warn("startupDelay was interrupted");
//						}
						try {
							connection[0] = FilemakerDialect.super.getConnection();
						} catch (SQLException e) {
							exceptions[0] = e;
						} finally {					
							latch.countDown(); // Release await() in the main thread.
						}
	        }
		};
		
		try {
			Thread t = new Thread(getConnectionWorker, "get-filemaker-jdbc-connection");
			t.start();
			if(latch.await(FilemakerDialect.getClientTimeoutSeconds(), TimeUnit.SECONDS)) { // Wait a bit longer as the login time-out set in the super class implementation
				if(connection[0] != null) {
					return connection[0];
				} else {
					if(exceptions[0] != null) {
						throw exceptions[0];
					} else {
						// this case should never happen, though
						throw new SQLException("Obtaining the connection failed due to an unknown reason.");
					}
				}
			} else {
				// TIMEOUT !!!!
				t.interrupt();
				throw new SQLException("Timeout after " + FilemakerDialect.getClientTimeoutSeconds() + " s - The TCP connection was established, but the Filemaker Server did not send further responses.");
			}
		} catch (InterruptedException e) {
			throw new SQLException(e);
		} catch (SQLException e) {
			throw e;
		}
	}
	
	@Override
	protected String currentTimestampDatabaseQuery() {
		return "SELECT CURRENT_TIMESTAMP FROM " + REFERENCE_TABLE_NAME + " FETCH FIRST 1 ROWS ONLY";
	}

	@Override
	protected String checkConnectionQuery() {
		return "SELECT * FROM " + REFERENCE_TABLE_NAME + " FETCH FIRST 1 ROWS ONLY";
	}

	/**
	 * Clone of the super class method in {@link GenericDatabaseDialect#describeColumns(Connection, String, String, String, String)}
	 * Modified code parts are commented.
	 */
	@Override
	public Map<ColumnId, ColumnDefinition> describeColumns(
	    Connection connection,
	    String catalogPattern,
	    String schemaPattern,
	    String tablePattern,
	    String columnPattern
	) throws SQLException {
	  logger.debug(
	      "Querying {} dialect column metadata for catalog:{} schema:{} table:{}",
	      this,
	      catalogPattern,
	      schemaPattern,
	      tablePattern
	  );

	  // Get the primary keys of the table(s) ...
	  final Set<ColumnId> pkColumns = primaryKeyColumns(
	      connection,
	      catalogPattern,
	      schemaPattern,
	      tablePattern
	  );
	  Map<ColumnId, ColumnDefinition> results = new HashMap<>();
	  try (ResultSet rs = connection.getMetaData().getColumns(
	      catalogPattern,
	      schemaPattern,
	      tablePattern,
	      columnPattern
	  )) {
	    final int rsColumnCount = rs.getMetaData().getColumnCount();
	    while (rs.next()) {
	      final String catalogName = rs.getString(1);
	      final String schemaName = rs.getString(2);
	      final String tableName = rs.getString(3);
	      final TableId tableId = new TableId(catalogName, schemaName, tableName);
	      final String columnName = rs.getString(4);
	      final ColumnId columnId = new ColumnId(tableId, columnName, null);
	      // ----------- fixing jdbc type mapping --------
	      JdbcTypeMapping jdbcTypeMapping = jdbcTypeMapping(rs);
	      final int jdbcType = jdbcTypeMapping.jdbcType; 
	      final String typeName = jdbcTypeMapping.typeName; // originally:  rs.getString(6);
	      // ----------------------------------------------------------
	      final int precision = rs.getInt(7);
	      final int scale = rs.getInt(9);
	      final String typeClassName = null;
	      Nullability nullability;
	      final int nullableValue = rs.getInt(11);
	      switch (nullableValue) {
	        case DatabaseMetaData.columnNoNulls:
	          nullability = Nullability.NOT_NULL;
	          break;
	        case DatabaseMetaData.columnNullable:
	          nullability = Nullability.NULL;
	          break;
	        case DatabaseMetaData.columnNullableUnknown:
	        default:
	          nullability = Nullability.UNKNOWN;
	          break;
	      }
	      Boolean autoIncremented = null;
	      if (rsColumnCount >= 23) {
	        // Not all drivers include all columns ...
	        String isAutoIncremented = rs.getString(23);
	        if ("yes".equalsIgnoreCase(isAutoIncremented)) {
	          autoIncremented = Boolean.TRUE;
	        } else if ("no".equalsIgnoreCase(isAutoIncremented)) {
	          autoIncremented = Boolean.FALSE;
	        }
	      }
	      Boolean signed = null;
	      Boolean caseSensitive = null;
	      Boolean searchable = null;
	      Boolean currency = null;
	      Integer displaySize = null;
	      boolean isPrimaryKey = pkColumns.contains(columnId);
	      if (isPrimaryKey) {
	        // Some DBMSes report pks as null
	        nullability = Nullability.NOT_NULL;
	      }
	      ColumnDefinition defn = columnDefinition(
	          rs,
	          columnId,
	          jdbcType,
	          typeName,
	          typeClassName,
	          nullability,
	          Mutability.UNKNOWN,
	          precision,
	          scale,
	          signed,
	          displaySize,
	          autoIncremented,
	          caseSensitive,
	          searchable,
	          currency,
	          isPrimaryKey
	      );
	      results.put(columnId, defn);
	    }
	    return results;
	  }
	}
	
	/**
	 * Clone of the super class method in {@link GenericDatabaseDialect#describeColumnsByQuerying(Connection, TableId)}
	 * Modified code parts are commented.
	 */
	@Override
	  protected ColumnDefinition describeColumn(
	      ResultSetMetaData rsMetadata,
	      int column
	  ) throws SQLException {
	    String catalog = rsMetadata.getCatalogName(column);
	    String schema = rsMetadata.getSchemaName(column);
	    String tableName = rsMetadata.getTableName(column);
	    TableId tableId = new TableId(catalog, schema, tableName);
	    String name = rsMetadata.getColumnName(column);
	    String alias = rsMetadata.getColumnLabel(column);
	    ColumnId id = new ColumnId(tableId, name, alias);
	    Nullability nullability;
	    switch (rsMetadata.isNullable(column)) {
	      case ResultSetMetaData.columnNullable:
	        nullability = Nullability.NULL;
	        break;
	      case ResultSetMetaData.columnNoNulls:
	        nullability = Nullability.NOT_NULL;
	        break;
	      case ResultSetMetaData.columnNullableUnknown:
	      default:
	        nullability = Nullability.UNKNOWN;
	        break;
	    }
	    Mutability mutability = Mutability.MAYBE_WRITABLE;
	    if (rsMetadata.isReadOnly(column)) {
	      mutability = Mutability.READ_ONLY;
	    } else if (rsMetadata.isWritable(column)) {
	      mutability = Mutability.MAYBE_WRITABLE;
	    } else if (rsMetadata.isDefinitelyWritable(column)) {
	      mutability = Mutability.WRITABLE;
	    }
	    JdbcTypeMapping jdbcTypeMapping = jdbcTypeMapping(
	    		rsMetadata.getColumnType(column), 
	    		rsMetadata.getColumnClassName(column), 
	    		rsMetadata.getColumnTypeName(column), 
	    		rsMetadata.getScale(column), 
	    		rsMetadata.getPrecision(column)
	    		);
	    return new ColumnDefinition(
	        id,
	        // ----------- fixing jdbc type mapping --------
	        jdbcTypeMapping.jdbcType,
	        jdbcTypeMapping.typeName, // rsMetadata.getColumnTypeName(column),
	        jdbcTypeMapping.classNameForType, // rsMetadata.getColumnClassName(column),
	        // ---------------------------------------------
	        nullability,
	        mutability,
	        rsMetadata.getPrecision(column),
	        rsMetadata.getScale(column),
	        rsMetadata.isSigned(column),
	        rsMetadata.getColumnDisplaySize(column),
	        rsMetadata.isAutoIncrement(column),
	        rsMetadata.isCaseSensitive(column),
	        rsMetadata.isSearchable(column),
	        rsMetadata.isCurrency(column),
	        false
	    );
	  }

	/**
	 * Provides better mapping of filemaker types to Jdbc Types
	 * 
	 * see https://git.zkm.de/data-infrastructure/kafka-connect-jdbc-filemaker/-/issues/1/
	 */
	private JdbcTypeMapping jdbcTypeMapping(ResultSet rs) throws SQLException {
			
		// field id calculated as from 0-based index + 1 for 1 based column index:
		final int FIELD_DATA_TYPE = 4 + 1;      
		final int FIELD_TYPE_NAME = 5 + 1;     
		final int FIELD_DECIMAL_DIGITS = 8 + 1;
		final int FIELD_NUM_PREC_RADIX = 9 + 1;
		final int FIELD_SQL_DATA_TYPE = 13 + 1;	

		int decimalDigits = rs.getInt(FIELD_DECIMAL_DIGITS);
		int numPrecRadix = rs.getInt(FIELD_NUM_PREC_RADIX);
		String typeName = rs.getString(FIELD_TYPE_NAME); 
		String classNameForType = rs.getString(FIELD_SQL_DATA_TYPE); // only number ????
		
		logger.trace("FIELD_DATA_TYPE: " + rs.getInt(FIELD_DATA_TYPE) 
			+ ", FIELD_TYPE_NAME:" + typeName
			+ ", FIELD_DECIMAL_DIGITS: " + decimalDigits
			+ ", FIELD_NUM_PREC_RADIX: " + numPrecRadix
			+ ", FIELD_CLASS_NAME_FOR_TYPE: " + classNameForType
		);
		int filemakerJdbcType = rs.getInt(5);
		
		return jdbcTypeMapping(filemakerJdbcType, classNameForType, typeName, decimalDigits, numPrecRadix);
	}

	protected JdbcTypeMapping jdbcTypeMapping(int filemakerJdbcType, String classNameForType, String typeName,  int decimalDigits, int numPrecRadix) {
		if(filemakerJdbcType == Types.DOUBLE){
			// need more information for better mapping
			if(decimalDigits < 0 && numPrecRadix == 10) {
				filemakerJdbcType = Types.INTEGER;
			}
		}
		if(filemakerJdbcType == Types.DATE){
			// SQL DATE type would be mapped my kafka-connect to the connect Date type which requires time fields
			// As Filemaker Date is purely date information we need to map it to NVARCHAR in order to let 
			// it treat as String in 
			// by this we are avoiding:
			// org.apache.kafka.connect.errors.DataException: Kafka Connect Date type should not have any time fields set to non-zero values.
			filemakerJdbcType = Types.VARCHAR;
			typeName = "varchar";
			classNameForType = String.class.getName();
		}
		return new JdbcTypeMapping(filemakerJdbcType, typeName, classNameForType);
	}
	
	class JdbcTypeMapping {
		int jdbcType;
		String typeName;
		String classNameForType;
		public JdbcTypeMapping(int jdbcType, String typeName, String classNameForType) {
			this.jdbcType = jdbcType;
			this.typeName = typeName;
			this.classNameForType = classNameForType;
		}
		
	}

	
	/*
	 * [10]	Field  (id=211)	
			conn	J3Connection  (id=50)	
			length	4	
			mod	0	
			name	"NULLABLE" (id=247)	
			type	4	

		[17]	Field  (id=241)	
			conn	J3Connection  (id=50)	
			length	-1	
			mod	0	
			name	"IS_NULLABLE" (id=294)	
			type	12	

	 */

//	@Override
//	protected boolean includeTable(TableId table) {
//		// FIXME only for debugging
//		return !table.tableName().equals("Adressen");
//	}
}
