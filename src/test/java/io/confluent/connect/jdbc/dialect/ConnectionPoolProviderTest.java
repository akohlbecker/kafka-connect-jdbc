package io.confluent.connect.jdbc.dialect;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;

public class ConnectionPoolProviderTest{
	
	@Test
	public void testSchemaHostPortKey() {
		assertEquals("filemaker://fms.zkm.test:2399/database_1e", ConnectionPoolProvider.singleton().databaseKey("jdbc:filemaker://fms.zkm.test:2399/database_1e?user=test&password=test&SocketTimeout=10000"));
		assertEquals("filemaker://fms.zkm.test/database_1e", ConnectionPoolProvider.singleton().databaseKey("jdbc:filemaker://fms.zkm.test/database_1e?user=test&password=test&SocketTimeout=10000"));
	}
	

}
