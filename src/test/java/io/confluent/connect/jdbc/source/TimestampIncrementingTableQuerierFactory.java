package io.confluent.connect.jdbc.source;

import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TimestampGranularity;
import io.confluent.connect.jdbc.source.TableQuerier.QueryMode;

public class TimestampIncrementingTableQuerierFactory {
	
	public static TimestampIncrementingTableQuerier newTimestampIncrementingTableQuerierTableMode(DatabaseDialect dialect, String name,
      String topicPrefix,
      List<String> timestampColumnNames,
      String incrementingColumnName,
      Map<String, Object> offsetMap, Long timestampDelay,
      TimeZone timeZone, String suffix,
      TimestampGranularity timestampGranularity) {
		return new TimestampIncrementingTableQuerier(dialect, QueryMode.TABLE, topicPrefix,
	      suffix, timestampColumnNames,
	       incrementingColumnName,
	      offsetMap, timestampDelay,
	       timeZone, suffix,
	      timestampGranularity);
	}

}
