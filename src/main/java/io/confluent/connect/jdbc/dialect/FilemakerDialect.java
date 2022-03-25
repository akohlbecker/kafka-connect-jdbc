package io.confluent.connect.jdbc.dialect;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.GenericDatabaseDialect;
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

	public FilemakerDialect(AbstractConfig config) {
		super(config);
	}

	public FilemakerDialect(AbstractConfig config, IdentifierRules defaultIdentifierRules) {
		super(config, defaultIdentifierRules);
	}

	@Override
	protected String currentTimestampDatabaseQuery() {
		try {
			List<TableId> tableIds = tableIds(getConnection());
			if (!tableIds.isEmpty()) {
				return "SELECT CURRENT_TIMESTAMP FROM " + tableIds.get(0).tableName() + " FETCH FIRST 1 ROWS ONLY";
			} else {
				glog.error("No tables in database.");
				throw new ConnectException("No tables in database, determine current timestamp.");
			}
		} catch (SQLException e) {
			throw new ConnectException("Cant read table metadata.");
		}
	}
	
	protected boolean includeTable(TableId table) {
		// FIXME only for debugging
		return !table.tableName().equals("Adressen");
	}
}
