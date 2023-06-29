package io.confluent.connect.jdbc.dialect;

import java.net.URI;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionPoolProvider {
	
  private static final int DEFAULT_POOL_SIZE = 2;

private static final Logger logger = LoggerFactory.getLogger(ConnectionPoolProvider.class);
	
	static private ConnectionPoolProvider instance = null;
	
	private Map<String, HikariDataSource> poolMap = new HashMap<>();
	private Map<String, Set<GenericDatabaseDialect>> dialectInstanceRegistry = new HashMap<>();
	
	public static ConnectionPoolProvider singleton() {
		if(instance == null) {
			instance = new ConnectionPoolProvider();
		}
		return instance;
	}
	
	/**
	 * @param jdbcUrl a jdbc data source uri like e.g.:
	 *   {@code jdbc:filemaker://<filemaker host IP address>/<databasename>?user=<databaseusername>&password=<databasepassword>}
  
	 * @return
	 * @throws SQLException 
	 * @throws MalformeduriException 
	 */
	public HikariDataSource getConnectionPool(Properties properties, GenericDatabaseDialect dialect) throws SQLException {
		String databaseKey = databaseKey(dialect.jdbcUrl);
		if(!poolMap.containsKey(databaseKey) || poolMap.get(databaseKey).isClosed()) {
			poolMap.put(databaseKey, initPool(properties, dialect));
			dialectInstanceRegistry.put(databaseKey, new HashSet<GenericDatabaseDialect>());
		}
		if(!dialectInstanceRegistry.get(databaseKey).contains(dialect)) {
			dialectInstanceRegistry.get(databaseKey).add(dialect);
		}
		return poolMap.get(databaseKey);
	}
	
	/**
	 * Un-register the dialect instance and close the pool in case it is no longer being used.
	 * 
	 * @param dialect
	 */
	public void release(GenericDatabaseDialect dialect) {
		String databaseKey = databaseKey(dialect.jdbcUrl);
		if(!dialectInstanceRegistry.containsKey(databaseKey)) {
			logger.warn("DatabaseDialect instance " + dialect + " is not registered here!");		
		}
		if(!dialectInstanceRegistry.get(databaseKey).remove(dialect)) {
			logger.warn("DatabaseDialect instance " + dialect + " is no longer registered for this jdbc uri " + dialect.sanitizedUrl(dialect.jdbcUrl) + ".");		
		}
		if(dialectInstanceRegistry.get(databaseKey).isEmpty()) {
			logger.info("No more DatabaseDialect instances for this databaseKey. Closing and removing pool now ...");
			poolMap.get(databaseKey).close();
			poolMap.remove(databaseKey);
			dialectInstanceRegistry.remove(databaseKey);
		}
	}

	private HikariDataSource initPool(Properties properties, GenericDatabaseDialect dialect) throws SQLException {
		logger.info("Creating new pool for " + dialect.identifier());
		final HikariConfig hikariConfig = new HikariConfig();
		hikariConfig.setJdbcUrl(dialect.jdbcUrl);
		hikariConfig.setMinimumIdle(0);
		hikariConfig.setMaximumPoolSize(DEFAULT_POOL_SIZE);
		hikariConfig.setDataSourceProperties(properties);
		if(!dialect.isJDBCv4Driver()) {
			hikariConfig.setConnectionTestQuery(dialect.checkConnectionQuery());
		}
		HikariDataSource dataSource = new HikariDataSource(hikariConfig);
		dataSource.setLoginTimeout(dialect.getLoginTimeout());
		return dataSource;
	}
	
	public int poolCount() {
		return poolMap.size();
	}

	protected String databaseKey(String jdbcUrl) {
		
		String cleanURI = jdbcUrl.substring(5);
		URI uri = URI.create(cleanURI);
		String key = uri.getScheme() + "://" + uri.getHost();
		if(uri.getPort() > -1) {
			key += ":" + uri.getPort();
		}
		key += uri.getPath();
		return key;
	}
}
