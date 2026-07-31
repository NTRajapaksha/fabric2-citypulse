# CityPulse: End-to-End Microsoft Fabric Step-by-Step Guide

This is a comprehensive, step-by-step guide to building the **CityPulse Urban Mobility Analytics** project from scratch in Microsoft Fabric. 

This project demonstrates **OneLake shortcuts** (zero-copy data virtualization) combined with Fabric's **built-in sample streaming data**, eliminating the need for external producer scripts.

> [!TIP]
> **Optimized Build Order:** This guide is ordered to minimize unnecessary trial capacity consumption. We will configure the static infrastructure (workspaces, lakehouses, and external shortcuts) *before* turning on the continuous streaming data sources.

---

## Prerequisites
Before you begin, ensure you have the following:
1. **Microsoft Fabric Trial**: A Fabric workspace (you can reuse ones from previous projects or start fresh).
2. **Azure Subscription**: An Azure Storage account with a Blob container (Azure free tier is sufficient).
3. **Reference Data Files**: 
   - `taxi_zone_lookup.csv` (NYC TLC Taxi Zone Lookup table)
   - Bike station reference data (e.g., TfL / Citi Bike station to neighborhood mapping). 
   *(Note: These are small, public CSVs.)*

---

## Phase 0: Workspace & Infrastructure Setup

We will simulate an organizational structure by separating our data across two workspaces (Bronze/Silver and Gold).
> *Why we do this: Separating Bronze/Silver (Data Engineering) from Gold (Business Intelligence) enforces strict access control and mimics real-world enterprise architectures where report builders shouldn't have access to raw, uncleansed data.*

### 1. Create Workspaces
1. Navigate to the Fabric portal.
2. Click **Workspaces** > **New workspace**.
3. Name it `CityPulse-Bronze` (Make sure it's assigned to a Fabric capacity/trial).
4. Create a second workspace named `CityPulse-Gold`.

### 2. Create Fabric Items
1. Open the `CityPulse-Bronze` workspace.
2. Click **New** > **Lakehouse**. Name it `lh_citypulse`.
3. Click **New** > **Eventhouse**. Name it `eh_citypulse`. It will create a default KQL database; rename it to `kql_citypulse`.
4. > **CRITICAL STEP**: Open the `kql_citypulse` database, click the **pencil icon** (Edit) or database settings, and turn **OneLake availability** to **ON**. Doing this *now* ensures all streaming tables you create later will sync to OneLake perfectly without getting stuck!
   > *Why we do this: KQL databases store data in a proprietary format. Enabling OneLake Availability forces the engine to automatically mirror the data into open-source Delta Parquet format in the background, making it readable by the rest of Fabric.*
5. Open the `CityPulse-Gold` workspace.
6. Click **New** > **Warehouse**. Name it `wh_citypulse_gold`.

### 3. Set Up External Storage (Azure Blob)
1. Go to the Azure Portal (portal.azure.com).
2. Create a new **Resource Group** (or use an existing one), then create a new **Storage Account** within it.
3. Create a new Blob container (e.g., `citypulse-reference-data`).
4. Upload your reference CSV files (`taxi_zone_lookup.csv` and your bike station CSV) into this container. 
   > **IMPORTANT**: Do NOT upload these directly into the Fabric Lakehouse. 
   > *Why we do this: The purpose of this step is to demonstrate OneLake's ability to virtualize and read data that lives entirely outside of the Fabric ecosystem without having to physically migrate or copy it.*

---

## Phase 1: Static Data & External Shortcut (Lakehouse → Azure Blob)

We will configure our static dimension data first, linking our external Azure reference data directly into OneLake without copying it.

1. Navigate to your Lakehouse `lh_citypulse` in `CityPulse-Bronze`.
2. In the Lakehouse explorer, locate the **Files** section (or **Tables** if the CSV structure is highly regular).
3. Right-click **Files** > **New shortcut**.
4. Choose **Azure Data Lake Storage Gen2** (this is compatible with Azure Blob).
5. Enter your Azure Storage account URL, connection credentials (Account Key or SAS token), and specify the container/path.
6. Select your reference CSV files.
7. If prompted to "Transform your data" to Delta, click **Skip**. 
   > *Why we do this: We want these to remain as raw CSV files so our PySpark notebook can explicitly parse and join them on the fly. It proves Fabric can dynamically mix raw external files with internal Delta tables.*
8. Verify you can now browse these files natively within the Lakehouse interface under the **Files** section.

---

## Phase 2: Streaming Ingestion (Built-In Sample Data)

Now that the static infrastructure is ready, we will turn on the live streams.
> *Why we do this: Real-time telemetry generates massive data. By routing it through Eventstreams into an Eventhouse (KQL) rather than a standard Lakehouse, we utilize an engine specifically optimized for high-throughput, time-series data.*

### 1. Create the Bicycles Eventstream
1. In `CityPulse-Bronze`, click **New** > **Eventstream**. Name it `es_bikes`.
2. In the Eventstream canvas, click **New source** > **Sample data**.
3. Select **Bicycles** from the sample data options.
4. **(Optional but recommended)**: Add a **Group by** transformation node on the canvas to pre-aggregate bike counts. Configure it exactly as follows:
   - **Aggregations**: Type = `Average`, Field = `No_Bikes`, Name = `Avg_No_Bikes`
   - **Settings**: Group aggregations by = `BikepointID, Neighborhood, Street, Latitude, Longitude`
   - **Time window**: `Tumbling`, Duration = `30` `Second`
   > *Why we do this: Aggregating 30-second windows drastically reduces the amount of raw data hitting our database while preserving all necessary location columns, saving compute costs.*
5. Click **New destination** > **Eventhouse**.
6. Select your workspace, then `eh_citypulse` (or `kql_citypulse` DB), and specify a new table name: `BikeStatus`.

### 2. Create the Yellow Taxi Eventstream
1. In `CityPulse-Bronze`, create another **Eventstream** named `es_taxi`.
2. Add a **New source** > **Sample data** > **Yellow Taxi**.
3. Add a **New destination** > **Eventhouse**. Select `eh_citypulse` and create a new table: `TaxiTrips`.
4. **Publish** both Eventstreams.

### 3. Validate Ingestion & Pause
1. Go to your Eventhouse `eh_citypulse` / `kql_citypulse`.
2. Open a **KQL Queryset**.
3. Run the following queries to ensure data is flowing:
   ```kusto
   BikeStatus | take 10
   TaxiTrips | take 10
   ```
4. > **CRITICAL**: Once you confirm data is flowing, go back to your `es_bikes` and `es_taxi` eventstreams and click **Stop/Pause**. 
   > *Why we do this: These built-in sample streams run 24/7 automatically and will drain your Fabric trial capacity while you build the rest of this project. You only need them running when testing the PySpark job or viewing the live dashboard.*

---

## Phase 3: Ingest to Silver (Direct KQL Connection)

Since Fabric Trial capacities often stall when syncing OneLake shortcuts in the background, we will bypass the shortcut entirely and pull data directly from the live Eventhouse engine!
> *Why we do this: The Kusto Spark Connector completely skips the physical disk (OneLake) and reads directly from the live KQL compute engine's memory, ensuring real-time data access without waiting for background batch syncs.*

### 1. Join and Enrich Data (Notebook)
> **NOTE**: Before running the notebook, go back to your `es_bikes` and `es_taxi` Eventstreams and click **Resume/Start** so fresh data is actively flowing.

1. Create a new **Notebook** named `nb_enrich_streams` attached to `lh_citypulse`.
2. Expand `kql_citypulse` -> `Tables` in the Explorer pane. Hover over `TaxiTrips`, click `...`, and click **Load data > Spark**. This will auto-generate the complex connection code with your unique `kustoUri`.
3. Consolidate and update the generated code to match the following robust enrichment logic:
   > *Why we do this: We use `.mode("overwrite")` because the KQL query pulls the full history every time. Overwriting ensures our Silver Delta tables perfectly mirror the database without duplicating records.*

   ```python
   from pyspark.sql.functions import col

   # 1. ENRICH TAXI DATA
   kustoQuery = "TaxiTrips"
   # REPLACE kustoUri WITH YOUR GENERATED URI
   kustoUri = "https://trd-xxxx.z2.kusto.fabric.microsoft.com"
   database = "kql_citypulse"

   accessToken = notebookutils.credentials.getToken(kustoUri)
   df_taxi = spark.read\
       .format("com.microsoft.kusto.spark.synapse.datasource")\
       .option("accessToken", accessToken)\
       .option("kustoCluster", kustoUri)\
       .option("kustoDatabase", database)\
       .option("kustoQuery", kustoQuery).load()

   df_zones = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Files/citypulse-reference-data/taxi_zone_lookup.csv")

   df_taxi_enriched = df_taxi.join(
       df_zones, df_taxi["DOLocationID"] == df_zones["LocationID"], "left"
   ).withColumnRenamed("Zone", "Dropoff_Zone").withColumnRenamed("Borough", "Dropoff_Borough").drop("LocationID", "service_zone")

   df_taxi_enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_taxi_trips")

   # 2. ENRICH BIKE DATA (Native London Stream Data)
   kustoQuery = "BikeStatus"
   df_bikes  = spark.read\
       .format("com.microsoft.kusto.spark.synapse.datasource")\
       .option("accessToken", accessToken)\
       .option("kustoCluster", kustoUri)\
       .option("kustoDatabase", database)\
       .option("kustoQuery", kustoQuery).load()

   # No CSV join needed! The London data already contains Street, Neighborhood, Latitude, and Longitude.
   df_bikes.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_bike_status")
   ```

### 3. Automate the Process (Data Pipeline)
1. In `CityPulse-Bronze`, click **New** > **Data Pipeline**. Name it `pl_enrich_mobility`.
2. Add a **Notebook Activity** to the canvas and point it to `nb_enrich_streams`.
3. Click the **Schedule** button on the top ribbon.
4. Set it to run on a cadence (e.g., every 15 minutes) to continually process new streaming rows.
   > *Why we do this: Automating the notebook execution transforms our manual PySpark script into a fully operational Data Pipeline that continuously hydrates the Silver layer.*
5. Click **Run** on the top ribbon to execute the pipeline manually right now. 
6. > **CRITICAL**: Once the pipeline run finishes successfully, go back and **Stop/Pause** your `es_bikes` and `es_taxi` eventstreams again! The data now sitting in your Silver tables is all you need to build the Warehouse and Historical reports.

---

## Phase 4: Cross-Workspace Shortcut (Warehouse → Lakehouse)

We will now mimic a central BI team consuming data prepared by the data engineering team.
> *Why we do this: Warehouses cannot natively hold OneLake shortcuts. By creating a proxy "Gold Lakehouse", we trick the Warehouse into allowing cross-database T-SQL queries over remote Silver Delta files without physically moving the data.*

1. Navigate to the `CityPulse-Gold` workspace and click **New** > **Lakehouse**. Name it `lh_citypulse_gold`.
2. In this new Lakehouse, right-click **Tables** > **New shortcut** > **Microsoft OneLake**.
3. Browse to the `CityPulse-Bronze` workspace > `lh_citypulse` > Tables, and select both `silver_taxi_trips` and `silver_bike_status`.
4. Now, switch to your Warehouse (`wh_citypulse_gold`) in the Gold workspace.
5. In the top left of the Explorer pane, click the **+ Warehouses** (or + Datawarehouses/Lakehouses) button.
6. A panel will appear showing items in your current workspace. Select the `lh_citypulse_gold` Lakehouse you just created. Click **Confirm**.
7. Open a new **SQL query** window in the Warehouse.
8. Use T-SQL to build a Gold layer star schema using Cross-Database querying. 
   > *Why we do this: Creating SQL Views allows us to rename columns and build strict Fact/Dimension tables (Star Schema) on the fly, providing a clean semantic layer for Power BI without duplicating storage.*

   **Fact Tables**:
   ```sql
   -- 1. Taxi Trips Fact Table
   CREATE VIEW dbo.fact_taxi_trips AS
   SELECT 
       DOLocationID AS DropoffLocationID,
       tpep_pickup_datetime AS PickupTime,
       tpep_dropoff_datetime AS DropoffTime,
       trip_distance AS TripDistance,
       total_amount AS TotalFare
   FROM lh_citypulse_gold.dbo.silver_taxi_trips;
   GO

   -- 2. Bike Status Fact Table
   CREATE VIEW dbo.fact_bike_status AS
   SELECT 
       BikepointID,
       Window_End_Time AS StatusTime,
       Avg_No_Bikes AS AvailableBikes
   FROM lh_citypulse_gold.dbo.silver_bike_status;
   GO
   ```

   **Dimension Tables**:
   ```sql
   -- 3. Taxi Zone Dimension (Extract unique zones)
   CREATE VIEW dbo.dim_taxi_zone AS
   SELECT DISTINCT 
       DOLocationID AS LocationID,
       Dropoff_Borough AS Borough,
       Dropoff_Zone AS Zone
   FROM lh_citypulse_gold.dbo.silver_taxi_trips
   WHERE DOLocationID IS NOT NULL;
   GO

   -- 4. Bike Station Dimension (Extract unique stations)
   CREATE VIEW dbo.dim_bike_station AS
   SELECT DISTINCT 
       BikepointID,
       Street AS StationName,
       Neighborhood,
       Latitude,
       Longitude
   FROM lh_citypulse_gold.dbo.silver_bike_status
   WHERE BikepointID IS NOT NULL;
   GO
   ```

---

## Phase 5: Semantic Model & Power BI (Direct Lake)

1. While in `wh_citypulse_gold`, click the **New semantic model** button from the ribbon.
2. Select your fact and dimension tables.
3. **Create Relationships**: In the Model view, drag and drop to connect your Dimension tables to your Fact tables (1-to-Many).
4. Create the following **DAX measures** in your model:
   ```dax
   Total Taxi Trips = COUNTROWS('fact_taxi_trips')
   Avg Taxi Fare = AVERAGE('fact_taxi_trips'[TotalFare])
   Avg Bike Availability = AVERAGE('fact_bike_status'[AvailableBikes])
   ```
5. Click **Create report** (Power BI).
   > *Why we do this: This model automatically uses **Direct Lake** mode. Instead of executing slow SQL queries (DirectQuery) or copying data into memory (Import), Direct Lake streams the Parquet files directly into Power BI's memory, giving you blazing fast performance with zero duplication.*
6. **Build Page 1 (Historical/Context)**:
   - **Line Chart**: `Average Taxi Fare Over Time`
   - **Bar Chart**: `Taxi Trips by Dropoff Borough`
   - **Map Visual**: `Average Bike Availability by Station`

---

## Phase 6: Real-Time Dashboard & Data Activator

> *Why we do this: Power BI is fantastic for deep historical analysis, but it isn't designed for sub-second, blinking-light operational screens. Real-Time Dashboards run natively on KQL and are built specifically for live ops.*

1. Go back to `CityPulse-Bronze`.
2. Click **New** > **Real-Time Dashboard**.
3. Connect it to your `eh_citypulse` Eventhouse.
4. Create the following KQL-backed visuals for your live operations center:
   - **Visual 1 (Stat / Gauge)**: 
     - *Title*: `Current Taxi Trips (Last 5 Mins)`
     - *KQL Query*: `TaxiTrips | where todatetime(tpep_pickup_datetime) > ago(5m) | count`
   - **Visual 2 (Time-Series Line Chart)**: 
     - *Title*: `Live Bike Availability Trend`
     - *KQL Query*: `BikeStatus | summarize AvgBikes = avg(Avg_No_Bikes) by bin(todatetime(Window_End_Time), 1m)`
5. From the Real-Time Dashboard, highlight the Bike Availability visual and click **Set Alert** (Data Activator).
6. Configure the condition: *If `AvgBikes` drops below 5, trigger an alert (email or Teams message).*
   > *Why we do this: Data Activator continuously monitors the Real-Time query in the background and takes automated actions when business rules are broken, completing the "Active" part of the data platform.*

---

## Phase 7: Governance & Cleanup

### 1. View the Shortcut Graph
1. Go to the Fabric homepage or OneLake Data Hub.
2. View the **Lineage** for your `wh_citypulse_gold` Warehouse or Semantic Model.
   > *Why we do this: The lineage graph visually proves our zero-copy architecture, showing a clean trace from the external Azure Blob, through the Eventhouse and Lakehouse, directly into your Warehouse.*

### 2. IMPORTANT: Pause Eventstreams
> **WARNING**: Fabric's built-in sample data sources run continuously and will consume your Capacity Units (CU) indefinitely if left unchecked.

1. Navigate to `es_bikes` and `es_taxi` in `CityPulse-Bronze`.
2. Click **Stop** or **Pause** on the eventstreams when you are done testing to preserve your trial capacity. You can always restart them later instantly.
