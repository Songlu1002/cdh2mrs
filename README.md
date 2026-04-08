# Hadoop Migration Tool

A lightweight tool for migrating data from CDH 7.1.9 to Huawei MRS 3.5.0.

## Features

- DistCp-based data migration via WebHDFS
- YAML-based configuration
- State persistence with JSON
- Configurable error handling
- Migration report generation

## Requirements

- Java 11+
- Hadoop client (HADOOP_HOME set)
- WebHDFS enabled on both clusters
- Network access to cluster HDFS ports

## Build

```bash
mvn clean package
```

## Usage

```bash
java -jar target/hadoop-migration-tool-1.0.0-SNAPSHOT.jar --config conf/config.yaml
```

## Configuration

See `src/main/resources/config.yaml` for full configuration options.

## Status

**MVP Phase**: Data migration only. Metadata migration in development.
