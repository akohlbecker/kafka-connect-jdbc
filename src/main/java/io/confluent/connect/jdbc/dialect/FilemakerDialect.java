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

	private String referenceTableName = null;
	private Integer lastConnectioHashCode = null;

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
				// TIMOUT !!!!
				t.interrupt();
				throw new SQLException("Timeout after " + FilemakerDialect.getClientTimeoutSeconds() + " s - The TCP connection was established, but the Filemaker Server did not send further responses.");
			}
		} catch (InterruptedException e) {
			throw new SQLException(e);
		} catch (SQLException e) {
			throw e;
		}
	}


	private Properties getConnectionProperties(String username, Password dbPassword) {
		Properties properties = new Properties();
		if (username != null) {
			properties.setProperty("user", username);
		}
		if (dbPassword != null) {
			properties.setProperty("password", dbPassword.value());
		}
		properties = addConnectionProperties(properties);
		return properties;
	}
	
	/**
	 * Tries to find the first table in the Filemaker database file which not empty
	 * and not corrupt and returns the according table name as reference for all
	 * subsequent operations with this connection.
	 * 
	 * @return
	 * @throws ConnectException
	 */
	private String referenceTableName() throws ConnectException {
		try {
			Connection connection = getConnection();
			if (referenceTableName == null || lastConnectioHashCode == null
					|| lastConnectioHashCode.intValue() != connection.hashCode()) {
				List<TableId> tableIds = tableIds(getConnection());
				if (!tableIds.isEmpty()) {
					for (TableId tableId : tableIds) {
						referenceTableName = tableId.tableName();
						try (Statement statement = connection.createStatement()) {
							if (statement.execute(checkConnectionQuery(referenceTableName))) {
								ResultSet rs = null;
								try {
									// do nothing with the result set
									rs = statement.getResultSet();
									// table is ok!
									lastConnectioHashCode = connection.hashCode();
									break;
								} finally {
									if (rs != null) {
										rs.close();
									}
								}
							}
						} catch (SQLException e) {
							// try next table
						}
					}
				} else {
					logger.error("No tables in database.");
					throw new ConnectException("No tables in database, determine current timestamp.");
				}
			}
			return referenceTableName;
		} catch (SQLException e) {
			throw new ConnectException("Cant read table metadata.");
		}
	}

	@Override
	protected String currentTimestampDatabaseQuery() {
		return "SELECT CURRENT_TIMESTAMP FROM " + referenceTableName() + " FETCH FIRST 1 ROWS ONLY";
	}

	@Override
	protected String checkConnectionQuery() {
		return checkConnectionQuery(referenceTableName());
	}

	private String checkConnectionQuery(String referenceTableName) {
		return "SELECT * FROM " + referenceTableName + " FETCH FIRST 1 ROWS ONLY";
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
	      final int jdbcType = jdbcType(rs);
	      // ----------------------------------------------------------
	      final String typeName = rs.getString(6);
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
	 * Provides better mapping of filemaker types to Jdbc Types
	 * 
	 * see https://git.zkm.de/data-infrastructure/kafka-connect-jdbc-filemaker/-/issues/1/
	 */
	private int jdbcType(ResultSet rs) throws SQLException {
		
		// field id calcalculated as from 0-based index + 1 for 1 based column index:
		final int FIELD_DATA_TYPE = 4 + 1;      // type	4 = INT	
		final int FIELD_TYPE_NAME = 5 + 1;      // type	12 = VARCHAR	
		final int FIELD_DECIMAL_DIGITS = 8 + 1; // type	4 = INT	
		final int FIELD_NUM_PREC_RADIX = 9 + 1; // type	4 = INT	
		final int FIELD_SQL_DATA_TYPE = 13 + 1; // type	12 = VARCHAR	

		logger.trace("FIELD_DATA_TYPE: " + rs.getInt(FIELD_DATA_TYPE) 
			+ ", FIELD_TYPE_NAME:" + rs.getString(FIELD_TYPE_NAME) 
			+ ", FIELD_DECIMAL_DIGITS: " + rs.getInt(FIELD_DECIMAL_DIGITS)
			+ ", FIELD_NUM_PREC_RADIX: " + rs.getInt(FIELD_NUM_PREC_RADIX)
			+ ", FIELD_SQL_DATA_TYPE: " + rs.getString(FIELD_SQL_DATA_TYPE)
		);
		int filemakerJdbcType = rs.getInt(5);
		if(filemakerJdbcType == Types.DOUBLE){
			// need more information for better mapping
			if(rs.getInt(FIELD_DECIMAL_DIGITS) < 0 && rs.getInt(FIELD_NUM_PREC_RADIX) == 10) {
				filemakerJdbcType = Types.INTEGER;
			}
		}
		return filemakerJdbcType;
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
