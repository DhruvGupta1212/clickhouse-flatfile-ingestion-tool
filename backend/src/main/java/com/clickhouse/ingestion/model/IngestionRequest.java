package com.clickhouse.ingestion.model;

import lombok.Data;
import java.util.List;

@Data
public class IngestionRequest {
    private String sourceType; // "CLICKHOUSE" or "FLATFILE"
    private String targetType; // "CLICKHOUSE" or "FLATFILE"
    private ClickHouseConfig clickHouseConfig;
    private FlatFileConfig flatFileConfig;
    private List<String> selectedColumns;
    private String tableName; // For ClickHouse target
} 