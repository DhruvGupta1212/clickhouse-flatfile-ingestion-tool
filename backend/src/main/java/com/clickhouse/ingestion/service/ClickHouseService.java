package com.clickhouse.ingestion.service;

import com.clickhouse.ingestion.model.ClickHouseConfig;
import java.util.List;
import java.util.Map;

public interface ClickHouseService {
    boolean testConnection(ClickHouseConfig config);
    List<String> getTables(ClickHouseConfig config);
    List<String> getColumns(ClickHouseConfig config, String tableName);
    List<Map<String, Object>> previewData(ClickHouseConfig config, String tableName, List<String> columns, int limit);
    long ingestToFlatFile(ClickHouseConfig config, String tableName, List<String> columns, String filePath);
    long ingestFromFlatFile(ClickHouseConfig config, String tableName, List<String> columns, String filePath);
    List<Map<String, Object>> executeJoinQuery(ClickHouseConfig config, List<String> tables, String joinCondition, List<String> columns);
} 