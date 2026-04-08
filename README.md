# Hadoop Migration Tool (CDH to MRS)
# Hadoop 迁移工具 (CDH 到 MRS)

---

## English | [中文](#中文)

### Overview

A lightweight tool for migrating data from **CDH 7.1.9 (Hive 2.1.1)** to **Huawei MRS 3.5.0 (Hive 3.1.0)**. Migrates Parquet data files and Hive metadata with Hive version compatibility transformation.

### Features

| Feature | Description |
|---------|-------------|
| **DistCp Data Migration** | Migrates data files via WebHDFS protocol |
| **Hive Metadata Migration** | Extracts metadata from CDH HMS and creates tables in MRS HMS |
| **Version Compatibility** | Automatic transformation from Hive 2.x to 3.x |
| **Kerberos Authentication** | Supports Kerberos-authenticated clusters |
| **State Persistence** | Resume after interruption via JSON state file |
| **Report Generation** | JSON and HTML migration reports |
| **Progress Tracking** | Real-time progress display during migration |

### Architecture

```
+------------------+     +------------------+
|   CDH 7.1.9     |     |   MRS 3.5.0     |
|   Hive 2.1.1    |     |   Hive 3.1.0    |
+--------+---------+     +--------+---------+
         |                        |
         |   WebHDFS/REST API    |
         +----------+------------+
                    |
         +----------v------------+
         |  Migration Tool      |
         |  - DistCpExecutor    |
         |  - HiveMetadataExtractor |
         |  - CompatibilityTransformer |
         |  - HiveMetadataImporter |
         |  - ReportGenerator   |
         +---------------------+
```

### Requirements

| Component | Requirement |
|-----------|-------------|
| Java | 11 or higher |
| Hadoop | CDH and MRS clients configured |
| Network | Access to both clusters' WebHDFS ports |
| WebHDFS | Must be enabled on both clusters |
| Kerberos | Keytab and krb5.conf if Kerberos auth |

### Build

```bash
# Build with Maven Wrapper (recommended)
./mvnw clean package

# Or use Maven directly if installed
mvn clean package

# The JAR will be at: target/hadoop-migration-tool-1.0.0-SNAPSHOT.jar
```

#### Build Options

```bash
# Skip tests (faster build)
./mvnw clean package -DskipTests

# Run tests only
./mvnw test

# Build with test reports
./mvnw clean package test
```

#### Output

After build, you will find:

| File | Description |
|------|-------------|
| `target/hadoop-migration-tool-1.0.0-SNAPSHOT.jar` | Executable JAR with all dependencies |
| `target/surefire-reports/` | Test reports |
| `state/` | Migration state files (created at runtime) |
| `reports/` | Migration reports (created at runtime) |

### Configuration

Edit `conf/config.yaml`:

```yaml
clusters:
  source:
    name: "cdh-cluster"
    type: "CDH"
    version: "7.1.9"
    hiveVersion: "2.1.1"
    hdfs:
      protocol: "webhdfs"
      namenode: "cdh-namenode.example.com"
      port: 9870
    kerberos:
      enabled: true
      principal: "hadoop@REALM"
      keytabPath: "/path/to/keytab"
      krb5Conf: "/path/to/krb5.conf"

  target:
    name: "mrs-cluster"
    type: "MRS"
    version: "3.5.0"
    hiveVersion: "3.1.0"
    hdfs:
      protocol: "webhdfs"
      namenode: "mrs-namenode.example.com"
      port: 9870
    kerberos:
      enabled: true
      principal: "hadoop@REALM"
      keytabPath: "/path/to/keytab"
      krb5Conf: "/path/to/krb5.conf"

migration:
  tasks:
    # Migrate all tables in a database
    - database: "sales_db"
      tables: ["all"]

    # Migrate specific tables
    - database: "web_db"
      tables: ["users", "orders", "products"]

  distcp:
    mapTasks: 20
    bandwidthMB: 100
    retryCount: 3

  metadata:
    autoConvert: true
    skipUnsupportedProperties: true
    rewriteLocations: true

  execution:
    continueOnFailure: true
    maxFailedTasks: 10
    batchConcurrency: 1

  output:
    stateFile: "state/migration-state.json"
    reportDir: "reports/"
```

### Usage

#### Basic Migration (Data Only)

```bash
java -jar hadoop-migration-tool-1.0.0-SNAPSHOT.jar --config conf/config.yaml
```

#### With Metadata Migration

Enable `metadata.autoConvert: true` in config to migrate both data and table definitions.

#### Windows Launch Script

```cmd
bin\migration-tool.bat --config conf\config.yaml
```

### Migration Workflow

1. **Validate Configuration** - Check cluster connections
2. **Authenticate** - Kerberos login if enabled
3. **For Each Table:**
   - Extract metadata from CDH HMS (if metadata migration enabled)
   - Transform metadata for Hive 3.x compatibility
   - Execute DistCp to copy data files
   - Create table in MRS HMS
   - Verify table creation
4. **Generate Report** - JSON and HTML reports in `reports/`

### Report Output

After migration, reports are generated in `reports/`:

```
reports/
├── migration_report_20260408_143022.json
└── migration_report_20260408_143022.html
```

**JSON Report Contents:**
```json
{
  "migrationSummary": {
    "sourceCluster": "cdh-cluster",
    "targetCluster": "mrs-cluster",
    "totalTables": 150,
    "successfulTables": 148,
    "failedTables": 2,
    "totalDataSizeBytes": 361000000000,
    "totalDataSizeFormatted": "336.25 GB"
  },
  "tableDetails": [...]
}
```

### Hive Version Compatibility

| CDH (Hive 2.1.1) | MRS (Hive 3.1.0) | Transformation |
|-------------------|------------------|----------------|
| HDFS paths | HDFS paths | Rewrite `hdfs://source` → `hdfs://target` |
| Bucketing v1 | Bucketing v2 | Automatic upgrade |
| No transactional prop | Requires explicit | Add `transactional=false` |
| Statistics | Statistics | Clear old, recollect |
| UNIONTYPE | Not supported | Flag for manual |

### Supported Table Types

| Table Type | Supported | Notes |
|------------|-----------|-------|
| MANAGED_TABLE | Yes | Data copied to new warehouse path |
| EXTERNAL_TABLE | Yes | Data copied, location rewritten |
| VIEW | No | Flagged, requires manual migration |
| Materialized View | No | Not supported in Hive 2.x |

### Troubleshooting

| Issue | Solution |
|-------|----------|
| `Connection refused` | Enable WebHDFS on cluster |
| `Authentication error` | Check Kerberos ticket: `kinit -kt keytab principal` |
| `DistCp failed` | Check network between clusters |
| View migration | Views must be recreated manually |

---

## 中文

### 概述

一款轻量级工具，用于将数据从 **CDH 7.1.9 (Hive 2.1.1)** 迁移到 **华为 MRS 3.5.0 (Hive 3.1.0)**。支持 Parquet 数据文件迁移和 Hive 元数据转换。

### 功能特性

| 功能 | 说明 |
|------|------|
| **DistCp 数据迁移** | 通过 WebHDFS 协议迁移数据文件 |
| **Hive 元数据迁移** | 从 CDH HMS 提取元数据并在 MRS HMS 创建表 |
| **版本兼容性转换** | 自动处理 Hive 2.x 到 3.x 的兼容性问题 |
| **Kerberos 认证** | 支持 Kerberos 认证的集群 |
| **状态持久化** | 通过 JSON 状态文件支持中断恢复 |
| **报告生成** | 生成 JSON 和 HTML 格式迁移报告 |
| **进度跟踪** | 迁移过程中实时显示进度 |

### 系统架构

```
+------------------+     +------------------+
|   CDH 7.1.9     |     |   MRS 3.5.0     |
|   Hive 2.1.1    |     |   Hive 3.1.0    |
+--------+---------+     +--------+---------+
         |                        |
         |   WebHDFS/REST API    |
         +----------+------------+
                    |
         +----------v------------+
         |  迁移工具              |
         |  - DistCpExecutor    |
         |  - HiveMetadataExtractor |
         |  - CompatibilityTransformer |
         |  - HiveMetadataImporter |
         |  - ReportGenerator   |
         +---------------------+
```

### 环境要求

| 组件 | 要求 |
|------|------|
| Java | 11 或更高版本 |
| Hadoop | 配置好 CDH 和 MRS 客户端 |
| 网络 | 可访问两个集群的 WebHDFS 端口 |
| WebHDFS | 两个集群都必须启用 |
| Kerberos | 如需认证，提供 keytab 和 krb5.conf |

### 构建

```bash
# 使用 Maven Wrapper 构建（推荐）
./mvnw clean package

# 或使用已安装的 Maven
mvn clean package

# 生成的 JAR: target/hadoop-migration-tool-1.0.0-SNAPSHOT.jar
```

#### 构建选项

```bash
# 跳过测试（更快）
./mvnw clean package -DskipTests

# 仅运行测试
./mvnw test

# 构建并生成测试报告
./mvnw clean package test
```

#### 构建产物

构建完成后，产物位于:

| 文件 | 说明 |
|------|------|
| `target/hadoop-migration-tool-1.0.0-SNAPSHOT.jar` | 可执行 JAR（含所有依赖） |
| `target/surefire-reports/` | 测试报告 |
| `state/` | 迁移状态文件（运行时创建） |
| `reports/` | 迁移报告（运行时创建） |

### 配置示例

编辑 `conf/config.yaml`:

```yaml
clusters:
  source:
    name: "cdh-cluster"
    type: "CDH"
    version: "7.1.9"
    hiveVersion: "2.1.1"
    hdfs:
      protocol: "webhdfs"
      namenode: "cdh-namenode.example.com"
      port: 9870
    kerberos:
      enabled: true
      principal: "hadoop@REALM"
      keytabPath: "/path/to/keytab"
      krb5Conf: "/path/to/krb5.conf"

  target:
    name: "mrs-cluster"
    type: "MRS"
    version: "3.5.0"
    hiveVersion: "3.1.0"
    hdfs:
      protocol: "webhdfs"
      namenode: "mrs-namenode.example.com"
      port: 9870
    kerberos:
      enabled: true
      principal: "hadoop@REALM"
      keytabPath: "/path/to/keytab"
      krb5Conf: "/path/to/krb5.conf"

migration:
  tasks:
    # 迁移数据库中所有表
    - database: "sales_db"
      tables: ["all"]

    # 迁移指定表
    - database: "web_db"
      tables: ["users", "orders", "products"]

  distcp:
    mapTasks: 20          # DistCp 并行任务数
    bandwidthMB: 100      # 带宽限制 (MB/s)
    retryCount: 3        # 重试次数

  metadata:
    autoConvert: true     # 启用元数据迁移
    skipUnsupportedProperties: true
    rewriteLocations: true # 重写 HDFS 路径

  execution:
    continueOnFailure: true  # 失败后继续
    maxFailedTasks: 10
    batchConcurrency: 1

  output:
    stateFile: "state/migration-state.json"
    reportDir: "reports/"
```

### 使用方法

#### 基本迁移（仅数据）

```bash
java -jar hadoop-migration-tool-1.0.0-SNAPSHOT.jar --config conf/config.yaml
```

#### 启用元数据迁移

在配置中设置 `metadata.autoConvert: true` 以同时迁移表结构和数据。

#### Windows 启动脚本

```cmd
bin\migration-tool.bat --config conf\config.yaml
```

### 迁移流程

1. **配置验证** - 检查集群连接
2. **认证** - 如启用 Kerberos 则进行认证
3. **对每个表执行:**
   - 从 CDH HMS 提取元数据（如启用元数据迁移）
   - 转换元数据以适配 Hive 3.x
   - 执行 DistCp 复制数据文件
   - 在 MRS HMS 创建表
   - 验证表创建成功
4. **生成报告** - 在 `reports/` 目录生成 JSON 和 HTML 报告

### 报告输出

迁移完成后，在 `reports/` 目录生成报告:

```
reports/
├── migration_report_20260408_143022.json
└── migration_report_20260408_143022.html
```

**JSON 报告内容:**
```json
{
  "migrationSummary": {
    "sourceCluster": "cdh-cluster",
    "targetCluster": "mrs-cluster",
    "totalTables": 150,
    "successfulTables": 148,
    "failedTables": 2,
    "totalDataSizeBytes": 361000000000,
    "totalDataSizeFormatted": "336.25 GB"
  },
  "tableDetails": [...]
}
```

### Hive 版本兼容性

| CDH (Hive 2.1.1) | MRS (Hive 3.1.0) | 转换规则 |
|-------------------|------------------|----------|
| HDFS 路径 | HDFS 路径 | 重写 `hdfs://源集群` → `hdfs://目标集群` |
| Bucketing v1 | Bucketing v2 | 自动升级 |
| 无事务属性 | 需要显式设置 | 添加 `transactional=false` |
| 统计信息 | 统计信息 | 清除旧统计信息 |
| UNIONTYPE | 不支持 | 标记需手动处理 |

### 支持的表类型

| 表类型 | 支持 | 说明 |
|--------|------|------|
| MANAGED_TABLE | 是 | 数据复制到新 warehouse 路径 |
| EXTERNAL_TABLE | 是 | 数据复制，重写 location |
| VIEW | 否 | 标记后需手动重建 |
| 物化视图 | 否 | Hive 2.x 不支持 |

### 常见问题

| 问题 | 解决方案 |
|------|----------|
| `Connection refused` | 在集群上启用 WebHDFS |
| `Authentication error` | 检查 Kerberos 票据: `kinit -kt keytab principal` |
| `DistCp failed` | 检查集群间网络连接 |
| 视图迁移 | 视图需手动重建 |

### License

MIT License
