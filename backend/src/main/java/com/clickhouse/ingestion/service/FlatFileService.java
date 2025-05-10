package com.clickhouse.ingestion.service;

import com.clickhouse.ingestion.model.FlatFileConfig;
import java.util.List;
import java.util.Map;

public interface FlatFileService {
    List<String> getColumns(FlatFileConfig config);
    List<Map<String, Object>> previewData(FlatFileConfig config, List<String> columns, int limit);
    long getTotalRecords(FlatFileConfig config);
    boolean validateFile(FlatFileConfig config);
} 