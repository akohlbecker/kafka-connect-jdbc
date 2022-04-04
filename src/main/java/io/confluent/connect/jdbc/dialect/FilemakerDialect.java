package io.confluent.connect.jdbc.dialect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;

/**
 * Complies to Filemaker JDBC v.16
 * 
 * @author a.kohlbecker
 */
public class FilemakerDialect extends GenericDatabaseDialect {

	private static final Logger glog = LoggerFactory.getLogger(FilemakerDialect.class);

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
		int timeoutSeconds = 60;

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
			if(latch.await(timeoutSeconds, TimeUnit.SECONDS)) { // Wait a bit longer as the login time-out set in the super class implementation
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
				throw new SQLException("Timeout after " + timeoutSeconds + " s - The TCP connection was established, but the Filemaker Server did not send further responses.");
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
					glog.error("No tables in database.");
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
		return "SELECT  * FROM " + referenceTableName + " FETCH FIRST 1 ROWS ONLY";
	}

//	@Override
//	protected boolean includeTable(TableId table) {
//		// FIXME only for debugging
//		return !table.tableName().equals("Adressen");
//	}
}
