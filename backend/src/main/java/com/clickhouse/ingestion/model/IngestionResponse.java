package com.clickhouse.ingestion.model;

import lombok.Data;
import lombok.Builder;

@Data
@Builder
public class IngestionResponse {
    private boolean success;
    private String message;
    private long recordsProcessed;
    private String errorDetails;
} 