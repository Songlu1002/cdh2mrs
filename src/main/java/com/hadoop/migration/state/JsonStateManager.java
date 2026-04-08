package com.hadoop.migration.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.hadoop.migration.model.MigrationResult;
import com.hadoop.migration.model.MigrationState;
import com.hadoop.migration.model.MigrationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class JsonStateManager implements StateManager {
    private static final Logger log = LoggerFactory.getLogger(JsonStateManager.class);

    private final ObjectMapper mapper;
    private final String stateFilePath;
    private MigrationState state;

    public JsonStateManager(String stateFilePath) {
        this.stateFilePath = stateFilePath;
        this.mapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void initialize(String sourceCluster, String targetCluster) {
        this.state = new MigrationState();
        this.state.setSourceCluster(sourceCluster);
        this.state.setTargetCluster(targetCluster);
        log.info("Initialized migration state for {} -> {}",
            sourceCluster, targetCluster);
    }

    @Override
    public void saveState(MigrationState state) {
        this.state = state;
        try {
            Path path = Paths.get(stateFilePath);
            Files.createDirectories(path.getParent());
            mapper.writeValue(path.toFile(), state);
            log.debug("State saved to {}", stateFilePath);
        } catch (IOException e) {
            log.error("Failed to save state to {}", stateFilePath, e);
            throw new RuntimeException("Failed to persist state", e);
        }
    }

    @Override
    public MigrationState loadState() {
        try {
            Path path = Paths.get(stateFilePath);
            if (!Files.exists(path)) {
                return null;
            }
            this.state = mapper.readValue(path.toFile(), MigrationState.class);
            log.info("Loaded existing state from {}", stateFilePath);
            return this.state;
        } catch (IOException e) {
            log.error("Failed to load state from {}", stateFilePath, e);
            return null;
        }
    }

    @Override
    public void updateTableStatus(String database, String table, MigrationStatus status) {
        MigrationResult result = state.getResult(database, table);
        if (result == null) {
            result = MigrationResult.builder()
                .database(database)
                .table(table)
                .status(status)
                .build();
            state.putResult(database, table, result);
        } else {
            result = MigrationResult.builder()
                .database(database)
                .table(table)
                .status(status)
                .dataSize(result.getDataSizeBytes())
                .build();
            state.putResult(database, table, result);
        }
        saveState(state);
    }

    @Override
    public void recordResult(MigrationResult result) {
        state.putResult(result.getDatabase(), result.getTable(), result);
        saveState(state);
    }

    @Override
    public void markCompleted() {
        state.setEndTime(System.currentTimeMillis());
        saveState(state);
        log.info("Migration completed. Total: {}, Completed: {}, Failed: {}",
            state.getTotalCount(),
            state.getCompletedCount(),
            state.getFailedCount());
    }

    @Override
    public boolean hasState() {
        return Files.exists(Paths.get(stateFilePath));
    }

    public MigrationState getState() {
        return state;
    }
}