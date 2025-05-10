package com.clickhouse.ingestion.service.impl;

import com.clickhouse.ingestion.model.FlatFileConfig;
import com.clickhouse.ingestion.service.FlatFileService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
@Service
public class FlatFileServiceImpl implements FlatFileService {

    @Override
    public List<String> getColumns(FlatFileConfig config) {
        List<String> columns = new ArrayList<>();
        try (Reader reader = new FileReader(config.getFilePath());
             CSVParser csvParser = createCSVParser(reader, config)) {
            
            if (config.isHasHeader()) {
                columns.addAll(csvParser.getHeaderNames());
            } else {
                // If no header, create column names like col1, col2, etc.
                CSVRecord firstRecord = csvParser.iterator().next();
                for (int i = 0; i < firstRecord.size(); i++) {
                    columns.add("col" + (i + 1));
                }
            }
        } catch (Exception e) {
            log.error("Failed to get columns from flat file", e);
        }
        return columns;
    }

    @Override
    public List<Map<String, Object>> previewData(FlatFileConfig config, List<String> columns, int limit) {
        List<Map<String, Object>> results = new ArrayList<>();
        try (Reader reader = new FileReader(config.getFilePath());
             CSVParser csvParser = createCSVParser(reader, config)) {
            
            int count = 0;
            for (CSVRecord record : csvParser) {
                if (count >= limit) break;
                
                Map<String, Object> row = new HashMap<>();
                for (int i = 0; i < columns.size(); i++) {
                    row.put(columns.get(i), record.get(i));
                }
                results.add(row);
                count++;
            }
        } catch (Exception e) {
            log.error("Failed to preview flat file data", e);
        }
        return results;
    }

    @Override
    public long getTotalRecords(FlatFileConfig config) {
        try (Reader reader = new FileReader(config.getFilePath());
             CSVParser csvParser = createCSVParser(reader, config)) {
            
            long count = 0;
            for (CSVRecord record : csvParser) {
                count++;
            }
            return count;
        } catch (Exception e) {
            log.error("Failed to count records in flat file", e);
            return 0;
        }
    }

    @Override
    public boolean validateFile(FlatFileConfig config) {
        File file = new File(config.getFilePath());
        if (!file.exists() || !file.isFile()) {
            return false;
        }

        try (Reader reader = new FileReader(config.getFilePath());
             CSVParser csvParser = createCSVParser(reader, config)) {
            
            // Try to read first record to validate format
            csvParser.iterator().next();
            return true;
        } catch (Exception e) {
            log.error("Failed to validate flat file", e);
            return false;
        }
    }

    private CSVParser createCSVParser(Reader reader, FlatFileConfig config) throws IOException {
        CSVFormat format = CSVFormat.DEFAULT
            .withDelimiter(config.getDelimiter().charAt(0))
            .withHeader(config.isHasHeader() ? CSVFormat.DEFAULT.getHeader() : null)
            .withSkipHeaderRecord(config.isHasHeader());
        
        return new CSVParser(reader, format);
    }
} 