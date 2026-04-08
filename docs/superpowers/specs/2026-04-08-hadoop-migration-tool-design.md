# Hadoop Migration Tool - Design Specification

**Version**: 1.0
**Date**: 2026-04-08
**Project**: CDH 7.1.9 → Huawei MRS 3.5.0 Data Migration Tool

---

## 1. Overview

A lightweight tool for migrating Parquet data files and Hive metadata from CDH 7.1.9 (Hive 2.1.1) to Huawei MRS 3.5.0 (Hive 3.1.0). Target scale: hundreds of TB.

### 1.1 Key Design Decisions

| Decision | Choice |
|----------|--------|
| Language | Java 11 |
| Build Tool | Maven |
| Packaging | Shade JAR (uber-jar) |
| Logging | Logback |
| Project Structure | Single module |
| HMS Client | Hive 2.x Metastore Client |
| Testing | Unit tests with Mock |
| CLI Style | Config-file driven (`--config config.yaml`) |
| Execution Mode | Grouped batches with configurable concurrency |
| Metadata Strategy | Generate view DDL for manual migration |
| Cross-Cluster Protocol | WebHDFS on both ends |
| Error Handling | Continue on failure, generate failure report |
| State Persistence | Local JSON file |
| Implementation | MVP → v2 → v3 phased approach |

---

## 2. Technical Stack

### 2.1 Maven Coordinates

```xml
<groupId>com.hadoop.migration</groupId>
<artifactId>hadoop-migration-tool</artifactId>
<version>1.0.0-SNAPSHOT</version>
```

### 2.2 Key Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| Hive Metastore Client | 2.1.1 | HMS API for CDH source |
| Hadoop Common | 3.3.x | HDFS WebHDFS client |
| SnakeYAML | 2.x | YAML config parsing |
| Logback Classic | 1.4.x | Logging |
| Jackson | 2.15.x | JSON processing |
| JUnit 5 | 5.10.x | Unit testing |
| Mockito | 5.x | Mocking |

---

## 3. Project Structure

```
src/main/java/com/hadoop/migration/
├── Main.java                              # CLI entry point
├── config/
│   ├── AppConfig.java                     # Root config (YAML binding)
│   ├── ClusterConfig.java                 # Source/target cluster config
│   ├── KerberosConfig.java                # Kerberos authentication
│   ├── HdfsConfig.java                    # HDFS/WebHDFS config
│   ├── MigrationTask.java                 # Single migration task
│   ├── DistcpConfig.java                  # DistCp parameters
│   ├── MetadataConfig.java                # Metadata migration options
│   └── ExecutionConfig.java                # Execution options
├── model/
│   ├── TableMetadata.java                 # Hive table metadata model
│   ├── MigrationStatus.java               # Enum: PENDING/IN_PROGRESS/etc
│   ├── MigrationState.java                 # Overall migration state
│   ├── MigrationResult.java               # Single table migration result
│   └── CompatibilityIssue.java             # Compatibility issue record
├── executor/
│   ├── DistCpExecutor.java                # DistCp command execution
│   └── DataVerifier.java                  # Post-copy data verification
├── metadata/
│   ├── HiveMetadataExtractor.java          # Extract from CDH HMS
│   ├── HiveMetadataImporter.java           # Import to MRS HMS
│   └── CompatibilityTransformer.java       # Hive 2.x → 3.x transform
├── state/
│   ├── StateManager.java                  # State manager interface
│   └── JsonStateManager.java              # JSON file implementation
├── report/
│   └── ReportGenerator.java               # JSON/HTML report generation
└── exception/
    ├── MigrationException.java             # Base exception
    ├── ConfigurationException.java        # Config errors
    ├── AuthenticationException.java        # Kerberos/auth errors
    ├── ConnectionException.java            # Network errors
    ├── DistcpExecutionException.java      # DistCp failure
    ├── MetadataException.java              # Metadata errors
    └── RollbackException.java             # Rollback errors
```

---

## 4. Configuration Schema

### 4.1 config.yaml

```yaml
clusters:
  source:
    name: "cdh-cluster"
    type: "CDH"
    version: "7.1.9"
    hiveVersion: "2.1.1"
    configDir: "/path/to/cdh-client-conf"
    kerberos:
      enabled: true
      principal: "hadoop@REALM"
      keytabPath: "/path/to/cdh.keytab"
    hdfs:
      namenode: "cdh-nn.example.com"
      port: 9870
      protocol: "webhdfs"

  target:
    name: "mrs-cluster"
    type: "MRS"
    version: "3.5.0"
    hiveVersion: "3.1.0"
    configDir: "/path/to/mrs-client-conf"
    kerberos:
      enabled: true
      principal: "hadoop@MRS.REALM"
      keytabPath: "/path/to/mrs.keytab"
    hdfs:
      namenode: "mrs-nn.example.com"
      port: 9870
      protocol: "webhdfs"

migration:
  tasks:
    - database: "sales_db"
      tables:
        - "orders"
        - "customers"
    - database: "analytics_db"
      tables:
        - "all"

  distcp:
    mapTasks: 20
    bandwidthMB: 100
    retryCount: 3
    sourceProtocol: "webhdfs"
    targetProtocol: "webhdfs"

  metadata:
    migrateViews: false
    generateViewDdl: true
    unsupportedAction: "WARN"

  execution:
    continueOnFailure: true
    maxFailedTasks: 0
    batchConcurrency: 1

  output:
    stateFile: "state/migration-state.json"
    reportDir: "reports/"
    viewDdlFile: "reports/view-ddl.sql"
```

---

## 5. Execution Flow

### 5.1 State Machine

```
PENDING
   │
   ▼
DATA_COPYING ───────────────────────────────────────┐
   │                                                   │
   │ (success)                                         │ (failure)
   ▼                                                   ▼
DATA_COPIED ───────────────────────────────────→ FAILED
   │                                                   │
   │ (success)                                         │ (rollback)
   ▼                                                   ▼
METADATA_MIGRATING ────────────────────────────→ ROLLBACK
   │                                                   │
   │ (success)                                         │ (done)
   ▼                                                   ▼
COMPLETED                                        ROLLBACK_COMPLETE
```

### 5.2 State Definitions

| State | Description |
|-------|-------------|
| PENDING | Waiting to start |
| DATA_COPYING | DistCp in progress |
| DATA_COPIED | Data copy complete, waiting for metadata |
| METADATA_MIGRATING | Metadata migration in progress |
| COMPLETED | Migration successful |
| FAILED | Migration failed |
| ROLLBACK | Rolling back (cleanup) |
| ROLLBACK_COMPLETE | Rollback done |

### 5.3 Single Table Migration Sequence

1. Load configuration
2. Initialize Kerberos ticket (if enabled)
3. For each task in batch:
   a. Extract source table metadata via HMS client
   b. Execute DistCp (webhdfs:// → webhdfs://)
   c. Verify copied data (file count, size)
   d. Transform metadata for Hive 3.x compatibility
   e. Create table in MRS HMS
   f. Update state to COMPLETED
4. Generate migration report
5. Exit with appropriate code

---

## 6. DistCp Cross-Cluster Strategy

### 6.1 WebHDFS Protocol

Standard DistCp does NOT support direct cross-cluster copy. This implementation uses WebHDFS on both ends.

**Command construction:**
```bash
hadoop distcp \
  -skipcrccheck \
  -p \
  -update \
  -strategy dynamic \
  -m 20 \
  -bandwidth 100 \
  -webhdfs \
  webhdfs://cdh-nn:9870/source/path \
  webhdfs://mrs-nn:9870/target/path
```

**Critical parameters:**
| Parameter | Reason |
|-----------|--------|
| `-skipcrccheck` | Required for cross-cluster (different CRC impl) |
| `-p` | Preserve permissions, timestamps, block size |
| `-update` | Only copy files that differ |
| `-webhdfs` | Enable WebHDFS protocol |

### 6.2 Prerequisites

- Source CDH cluster: WebHDFS enabled (default on CDH 7.x)
- Target MRS cluster: WebHDFS enabled (default)
- Network: Windows PC must reach both clusters' WebHDFS ports

---

## 7. Exception Hierarchy

```
MigrationException (base)
├── ConfigurationException
├── AuthenticationException
│   ├── KerberosException
│   └── HdfsAccessException
├── ConnectionException
│   ├── SourceClusterException
│   └── TargetClusterException
├── DistcpExecutionException
│   ├── DistcpTimeoutException
│   └── DistcpCRCException
├── MetadataException
│   ├── ExtractionException
│   ├── TransformException
│   └── ImportException
├── DataVerificationException
├── RollbackException
└── StatePersistenceException
```

**Principles:**
- Each exception contains: error code, message, details, possible causes
- `DistcpExecutionException` includes list of copied files for rollback
- When `continueOnFailure: true`, exceptions are logged but do not halt execution

---

## 8. Report Format

### 8.1 JSON Report Structure

```json
{
  "reportVersion": "1.0",
  "generatedAt": "2026-04-08T14:30:22+08:00",
  "duration": {
    "startTime": "2026-04-08T10:00:00+08:00",
    "endTime": "2026-04-08T14:30:22+08:00",
    "totalMinutes": 270
  },
  "summary": {
    "totalTasks": 150,
    "completedTasks": 148,
    "failedTasks": 2,
    "skippedTasks": 0,
    "totalDataSizeBytes": 379657142345728,
    "totalDataSizeHuman": "345.4 TB"
  },
  "clusters": {
    "source": { "name": "cdh-cluster", "version": "7.1.9", "hiveVersion": "2.1.1" },
    "target": { "name": "mrs-cluster", "version": "3.5.0", "hiveVersion": "3.1.0" }
  },
  "failedTasks": [
    {
      "database": "sales_db",
      "table": "audit_logs",
      "error": "Unsupported column type: UNIONTYPE",
      "errorCode": "METADATA_TRANSFORM_001",
      "suggestion": "Manual conversion required"
    }
  ],
  "compatibilityIssues": {
    "autoResolved": 45,
    "requiresAttention": 3
  },
  "viewDdlFile": "reports/view-ddl.sql",
  "stateFile": "state/migration-state.json"
}
```

### 8.2 View DDL Output

```sql
-- ============================================================================
-- Generated View DDL for manual migration
-- Generated at: 2026-04-08T14:30:22+08:00
-- ============================================================================

CREATE VIEW IF NOT EXISTS `sales_db`.`order_summary` AS
SELECT order_id, customer_id, SUM(amount) AS total_amount
FROM `sales_db`.`orders`
GROUP BY order_id, customer_id;
-- NOTE: Verify all referenced UDFs are available on MRS
```

---

## 9. Phase Implementation Plan

### Phase 1: MVP
**Goal**: End-to-end data migration (no metadata)

- Project scaffold (Maven, Logback, Shade)
- Configuration management (YAML parsing)
- DistCp executor with WebHDFS
- Simple JSON state persistence
- CLI entry point with `--help`
- Basic logging

**Deliverable**: Single JAR that can copy one table's data

### Phase 2: Metadata Migration
**Goal**: Complete table migration (data + metadata)

- HMS Client integration
- Metadata extraction (CDH)
- Compatibility transformation (Hive 2.x → 3.x)
- Metadata import (MRS)
- Database/table creation

### Phase 3: Polish
**Goal**: Production-ready features

- View DDL generation
- Enhanced state persistence (resume)
- Complete report generation
- Error handling improvements
- Input validation

---

## 10. MVP Acceptance Criteria

1. Program loads config.yaml successfully
2. Executes DistCp to copy one table's data from CDH to MRS
3. State persists to JSON after completion
4. Logs record full execution
5. Exit code: 0 on success, non-zero on failure

---

## 11. Hive Compatibility Matrix

| Issue | Hive 2.1.1 | Hive 3.1.0 | Solution |
|-------|------------|------------|----------|
| HDFS paths | hdfs://cdh-nn:8020/ | hdfs://mrs-nn:8020/ | Rewrite paths |
| Bucketing | Version 1 | Version 2 | Upgrade |
| Transactional | None | Explicit | Add transactional=false |
| Statistics | Present | Discard | Clear stats |
| UNIONTYPE | Supported | Supported | Flag for manual |
| Views | Supported | Supported | Generate DDL |
| UDFs | Supported | Not migrated | Document separately |

---

## 12. Source Data Protection

For static migration, source data must not change during migration. Options:

| Method | Implementation |
|--------|---------------|
| HDFS Snapshot | `hdfs dfs -createSnapshot /path snapshot` |
| Read-only flag | `ALTER TABLE t SET TBLPROPERTIES ('readOnly'='true')` |
| Accept variance | Re-verify after migration |

**Note**: Source protection is operator responsibility; tool validates but does not enforce.
