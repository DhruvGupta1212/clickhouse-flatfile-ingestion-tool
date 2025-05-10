package com.clickhouse.ingestion.controller;

import com.clickhouse.ingestion.model.*;
import com.clickhouse.ingestion.service.ClickHouseService;
import com.clickhouse.ingestion.service.FlatFileService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class IngestionController {

    private final ClickHouseService clickHouseService;
    private final FlatFileService flatFileService;

    @PostMapping("/clickhouse/test-connection")
    public ResponseEntity<Boolean> testClickHouseConnection(@RequestBody ClickHouseConfig config) {
        return ResponseEntity.ok(clickHouseService.testConnection(config));
    }

    @PostMapping("/clickhouse/tables")
    public ResponseEntity<List<String>> getClickHouseTables(@RequestBody ClickHouseConfig config) {
        return ResponseEntity.ok(clickHouseService.getTables(config));
    }

    @PostMapping("/clickhouse/columns")
    public ResponseEntity<List<String>> getClickHouseColumns(
            @RequestBody ClickHouseConfig config,
            @RequestParam String tableName) {
        return ResponseEntity.ok(clickHouseService.getColumns(config, tableName));
    }

    @PostMapping("/clickhouse/preview")
    public ResponseEntity<List<Map<String, Object>>> previewClickHouseData(
            @RequestBody ClickHouseConfig config,
            @RequestParam String tableName,
            @RequestParam List<String> columns,
            @RequestParam(defaultValue = "100") int limit) {
        return ResponseEntity.ok(clickHouseService.previewData(config, tableName, columns, limit));
    }

    @PostMapping("/flatfile/validate")
    public ResponseEntity<Boolean> validateFlatFile(@RequestBody FlatFileConfig config) {
        return ResponseEntity.ok(flatFileService.validateFile(config));
    }

    @PostMapping("/flatfile/columns")
    public ResponseEntity<List<String>> getFlatFileColumns(@RequestBody FlatFileConfig config) {
        return ResponseEntity.ok(flatFileService.getColumns(config));
    }

    @PostMapping("/flatfile/preview")
    public ResponseEntity<List<Map<String, Object>>> previewFlatFileData(
            @RequestBody FlatFileConfig config,
            @RequestParam List<String> columns,
            @RequestParam(defaultValue = "100") int limit) {
        return ResponseEntity.ok(flatFileService.previewData(config, columns, limit));
    }

    @PostMapping("/ingest")
    public ResponseEntity<IngestionResponse> startIngestion(@RequestBody IngestionRequest request) {
        try {
            long recordsProcessed = 0;
            
            if ("CLICKHOUSE".equals(request.getSourceType()) && "FLATFILE".equals(request.getTargetType())) {
                recordsProcessed = clickHouseService.ingestToFlatFile(
                    request.getClickHouseConfig(),
                    request.getTableName(),
                    request.getSelectedColumns(),
                    request.getFlatFileConfig().getFilePath()
                );
            } else if ("FLATFILE".equals(request.getSourceType()) && "CLICKHOUSE".equals(request.getTargetType())) {
                recordsProcessed = clickHouseService.ingestFromFlatFile(
                    request.getClickHouseConfig(),
                    request.getTableName(),
                    request.getSelectedColumns(),
                    request.getFlatFileConfig().getFilePath()
                );
            }

            return ResponseEntity.ok(IngestionResponse.builder()
                .success(true)
                .message("Ingestion completed successfully")
                .recordsProcessed(recordsProcessed)
                .build());
        } catch (Exception e) {
            return ResponseEntity.ok(IngestionResponse.builder()
                .success(false)
                .message("Ingestion failed")
                .errorDetails(e.getMessage())
                .build());
        }
    }

    @PostMapping("/clickhouse/join")
    public ResponseEntity<List<Map<String, Object>>> executeJoinQuery(
            @RequestBody ClickHouseConfig config,
            @RequestParam List<String> tables,
            @RequestParam String joinCondition,
            @RequestParam List<String> columns) {
        return ResponseEntity.ok(clickHouseService.executeJoinQuery(config, tables, joinCondition, columns));
    }
} 