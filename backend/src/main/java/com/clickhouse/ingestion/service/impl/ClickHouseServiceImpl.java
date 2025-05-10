package com.clickhouse.ingestion.service.impl;

import com.clickhouse.ingestion.model.ClickHouseConfig;
import com.clickhouse.ingestion.service.ClickHouseService;
import com.clickhouse.jdbc.ClickHouseDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;

@Slf4j
@Service
public class ClickHouseServiceImpl implements ClickHouseService {

    @Override
    public boolean testConnection(ClickHouseConfig config) {
        try (Connection conn = createConnection(config)) {
            return conn.isValid(5);
        } catch (Exception e) {
            log.error("Connection test failed", e);
            return false;
        }
    }

    @Override
    public List<String> getTables(ClickHouseConfig config) {
        List<String> tables = new ArrayList<>();
        try (Connection conn = createConnection(config)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(config.getDatabase(), null, "%", new String[]{"TABLE"});
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        } catch (Exception e) {
            log.error("Failed to get tables", e);
        }
        return tables;
    }

    @Override
    public List<String> getColumns(ClickHouseConfig config, String tableName) {
        List<String> columns = new ArrayList<>();
        try (Connection conn = createConnection(config)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getColumns(config.getDatabase(), null, tableName, null);
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        } catch (Exception e) {
            log.error("Failed to get columns", e);
        }
        return columns;
    }

    @Override
    public List<Map<String, Object>> previewData(ClickHouseConfig config, String tableName, List<String> columns, int limit) {
        List<Map<String, Object>> results = new ArrayList<>();
        String columnList = String.join(", ", columns);
        String query = String.format("SELECT %s FROM %s LIMIT %d", columnList, tableName, limit);
        
        try (Connection conn = createConnection(config);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                results.add(row);
            }
        } catch (Exception e) {
            log.error("Failed to preview data", e);
        }
        return results;
    }

    @Override
    public long ingestToFlatFile(ClickHouseConfig config, String tableName, List<String> columns, String filePath) {
        // Implementation for ClickHouse to Flat File ingestion
        // This will be implemented in the next step
        return 0;
    }

    @Override
    public long ingestFromFlatFile(ClickHouseConfig config, String tableName, List<String> columns, String filePath) {
        // Implementation for Flat File to ClickHouse ingestion
        // This will be implemented in the next step
        return 0;
    }

    @Override
    public List<Map<String, Object>> executeJoinQuery(ClickHouseConfig config, List<String> tables, String joinCondition, List<String> columns) {
        List<Map<String, Object>> results = new ArrayList<>();
        String columnList = String.join(", ", columns);
        String tableList = String.join(" JOIN ", tables);
        String query = String.format("SELECT %s FROM %s WHERE %s", columnList, tableList, joinCondition);
        
        try (Connection conn = createConnection(config);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                results.add(row);
            }
        } catch (Exception e) {
            log.error("Failed to execute join query", e);
        }
        return results;
    }

    private Connection createConnection(ClickHouseConfig config) throws SQLException {
        String url = String.format("jdbc:clickhouse://%s:%d/%s", 
            config.getHost(), config.getPort(), config.getDatabase());
        
        Properties properties = new Properties();
        properties.setProperty("user", config.getUsername());
        if (config.getJwtToken() != null && !config.getJwtToken().isEmpty()) {
            properties.setProperty("jwt", config.getJwtToken());
        }
        
        return new ClickHouseDataSource(url, properties).getConnection();
    }
} 