# CityPulse: Urban Mobility Analytics Platform

**[🎥 View Project Demo Video / Live Dashboard Here](Insert-Link-Here)**

## Overview
CityPulse is a unified, zero-copy data analytics platform built entirely on Microsoft Fabric. It demonstrates a modern dual-speed data architecture (Medallion architecture) that processes live streaming telemetry alongside historical data analysis—all without unnecessary data duplication.

## Architecture Highlights
- **Bronze (Raw / Streaming)**: Ingests live telemetry (London Santander Cycles and NYC Yellow Taxis) using Fabric Eventstreams directly into a KQL Eventhouse, optimized for time-series data.
- **Silver (Enriched)**: Uses PySpark to query the live KQL engine and join the telemetry with static dimensional reference data (stored in an external Azure Blob Storage via OneLake shortcuts), writing the cleansed results into Delta Parquet format.
- **Gold (Serving)**: Utilizes a Fabric Data Warehouse and T-SQL views to project a pristine Star Schema (Fact and Dimension tables) over the remote Silver Lakehouse files via cross-workspace Zero-Copy Shortcuts.

## Dual-Speed Reporting
1. **Real-Time Operations**: A sub-second KQL Real-Time Dashboard monitors live bike availability and taxi drop-offs, with Data Activator configured for automated threshold alerting (e.g., if bike capacity drops critically low).
2. **Historical Analysis**: A Power BI Direct Lake Semantic Model provides blistering fast reporting over the Gold layer. By reading the Delta Parquet files directly from OneLake, it completely avoids traditional batch refresh schedules.

## Core Technologies Used
- **Platform**: Microsoft Fabric, OneLake
- **Data Engineering**: PySpark, Notebooks, Data Pipelines, Delta Lake
- **Streaming**: Fabric Eventstreams, Kusto Query Language (KQL Database)
- **Analytics & BI**: T-SQL (Fabric Warehouse), Power BI (Direct Lake), Data Activator

## Documentation
- 📖 **[Comprehensive Step-by-Step Guide](CityPulse-Step-By-Step-Guide.md)**: A detailed tutorial on how to recreate this entire architecture from scratch in your own Fabric environment. It includes deep-dives into the architectural reasoning and decisions made at each phase.
