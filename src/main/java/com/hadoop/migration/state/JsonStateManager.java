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
    private final Path stateFilePath;
    private MigrationState state;

    public JsonStateManager(String stateFilePath) {
        this.stateFilePath = Paths.get(stateFilePath).toAbsolutePath().normalize();
        this.mapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void initialize(String sourceCluster, String targetCluster) {
        // Check if state file already exists (resume scenario)
        if (stateFilePath.toFile().exists()) {
            try {
                this.state = mapper.readValue(stateFilePath.toFile(), MigrationState.class);
                log.info("Resuming migration from existing state file: {}", stateFilePath);
                log.info("  Source: {}, Target: {}", state.getSourceCluster(), state.getTargetCluster());
                log.info("  Already processed: {} tables, Completed: {}, Failed: {}",
                    state.getTotalCount(), state.getCompletedCount(), state.getFailedCount());

                // Verify clusters match
                if (!sourceCluster.equals(state.getSourceCluster()) ||
                    !targetCluster.equals(state.getTargetCluster())) {
                    log.warn("Cluster names in state file differ from config. " +
                        "State: {} -> {}, Config: {} -> {}",
                        state.getSourceCluster(), state.getTargetCluster(),
                        sourceCluster, targetCluster);
                }
                return;
            } catch (Exception e) {
                log.warn("Failed to load existing state file, creating new state: {}", e.getMessage());
            }
        }

        // Create new state for fresh migration
        this.state = new MigrationState();
        this.state.setSourceCluster(sourceCluster);
        this.state.setTargetCluster(targetCluster);
        saveState(state);
        log.info("Initialized new migration state for {} -> {} (persisted to {})",
            sourceCluster, targetCluster, stateFilePath);
    }

    @Override
    public void saveState(MigrationState state) {
        this.state = state;
        try {
            Path parentDir = stateFilePath.getParent();
            if (parentDir != null) {
                Files.createDirectories(parentDir);
            }
            mapper.writeValue(stateFilePath.toFile(), state);
            log.debug("State saved to {}", stateFilePath);

            // Verify the file was created
            if (!stateFilePath.toFile().exists()) {
                log.error("State file was not created despite no exception: {}", stateFilePath);
                throw new RuntimeException("State file was not created");
            }
        } catch (IOException e) {
            log.error("Failed to save state to {}", stateFilePath, e);
            throw new RuntimeException("Failed to persist state", e);
        }
    }

    @Override
    public MigrationState loadState() {
        // If we have in-memory state and file exists, prefer the file (for consistency)
        // But if file doesn't exist or load fails, return in-memory state if available
        if (!stateFilePath.toFile().exists()) {
            log.warn("State file does not exist at path: {}", stateFilePath);
            // Return in-memory state if available
            if (this.state != null) {
                log.info("Returning in-memory state (file not found)");
                return this.state;
            }
            return null;
        }
        try {
            this.state = mapper.readValue(stateFilePath.toFile(), MigrationState.class);
            log.info("Loaded existing state from {}", stateFilePath);
            return this.state;
        } catch (Exception e) {
            log.error("Failed to load state from {}: {}", stateFilePath, e.getMessage());
            // Try to return the in-memory state if available
            if (this.state != null) {
                log.warn("Returning in-memory state due to load failure: {}", e.getMessage());
                return this.state;
            }
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
            // Use withStatus to preserve existing information
            MigrationResult updatedResult = result.withStatus(status);
            state.putResult(database, table, updatedResult);
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
        return stateFilePath.toFile().exists();
    }

    public MigrationState getState() {
        return state;
    }

    /**
     * Check if a table has already been successfully migrated.
     * Used for resume functionality to skip already completed tables.
     */
    public boolean isTableCompleted(String database, String table) {
        MigrationResult result = state.getResult(database, table);
        return result != null && result.getStatus() == MigrationStatus.COMPLETED;
    }

    /**
     * Check if a table has already been processed (completed or failed).
     * Used for resume functionality to show status.
     */
    public boolean hasTableResult(String database, String table) {
        return state.getResult(database, table) != null;
    }
}