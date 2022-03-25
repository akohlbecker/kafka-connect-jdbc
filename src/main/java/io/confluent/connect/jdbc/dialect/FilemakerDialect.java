package io.confluent.connect.jdbc.dialect;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
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
