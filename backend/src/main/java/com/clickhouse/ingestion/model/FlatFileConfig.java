package com.clickhouse.ingestion.model;

import lombok.Data;

@Data
public class FlatFileConfig {
    private String filePath;
    private String delimiter;
    private boolean hasHeader;
} 