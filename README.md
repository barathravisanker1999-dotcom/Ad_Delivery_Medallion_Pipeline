## Ad Delivery Medallion Pipeline: Analysis & Scale-Up
### APPROACH
### README: Approach, Assumptions, & Ops Trade-Offs
#### **Bronze Layer: Ingestion & Schema Evolution**
- Persist raw data with minimal transformation
- Load events from two schema versions (v1 and v2) using Delta's `mergeSchema=true`
- Data pipeline can adapt automatically when the structure of incoming data changes, without breaking.
- Delta Lake with ACID transactional guarantees
#### **Silver Layer: Cleansing, Standardization & Deduplication**
- **Data Quality Gating**:
  - **Schema Unification**: Coalesce conflicting columns (e.g., `spend` → `media_cost`)
  - **Timestamp Standardization**: No matter how the date and time are received, convert them into one standard UTC time format
  - **Quarantine Bad Records**: Filter malformed timestamps or negative costs to a separate audit table
  **Deduplication Strategy**:
  ```sql
  Window Function: ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time_utc DESC)
  ```
  - Keeps the most recently ingested version of each event
  - Handles late-arriving data and reprocessing scenarios
  
- **Late-Arriving Data Handling**:
  - Uses Delta MERGE statement with ingest_time_utc timestamp comparison
  - Updates existing records only if source has newer timestamp
  - Preserves historical lineage via Delta versioning
#### **Gold Layer: Business Aggregation**
- **Grain**: Daily metrics per advertiser
- **Aggregations**: 
  - Event counts
  - Spend by advertiser and date
  - Currency conversion (INR → USD at fixed rate)
  - Left join with advertiser dimension for metadata
  
- **Output**: Structured for analytics queries

#### **Analytics (SQL Post-Processing)**
1. **Rolling metrics**: 7-day average spend, % change vs prior day
2. **Streaks detection**: Max consecutive days with activity per advertiser
3. **Unit testing**: Declarative data quality rules (null rates, uniqueness)
#### KEY ASSUMPTIONS & TRADE-OFFS

### Explicit Assumptions

| **Assumption**                           | **Impact**                                                                                                                                   | **Validation**                                                                                                  |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| **Timestamp formats are parseable**      | Invalid timestamp values become **NULL**, which may cause records to be skipped or processed incorrectly.                                    | Use **try-cast** to safely convert timestamps. Invalid records are stored in a **quarantine** table for review. |
| **event_id is globally unique**          | If two different events have the same **event_id**, deduplication may remove valid records, causing **data loss**.                           | No validation is implemented. The pipeline trusts that the source provides unique **event_id** values.          |
| **Fixed INR-to-USD rate (0.012)**        | Currency conversion may become inaccurate if the exchange rate changes, affecting business reports.                                          | The exchange rate is hard-coded. It should be updated daily or fetched in real time for production use.         |

### Technical Trade-Offs

1. Deduplication (Last-Write-Wins)
Current:
row_number().over(Window.partitionBy("event_id").orderBy(ingest_time_utc.desc()))

- ✅ Pro: Keeps the latest record and removes duplicates.
- ❌ Con: If the latest record is wrong, the correct older record is lost.
Alternative: Use version numbers to decide which record to keep.
--------------------------------------------------
2. Schema Drift
Current:
.option("mergeSchema", "true")

- ✅ Pro: Automatically adds new columns.
- ❌ Con: Schema changes may go unnoticed.
Alternative: Use schema versioning for better control.
--------------------------------------------------
3. Currency Conversion
Current:
INR_TO_USD = 0.012

- ✅ Pro: Simple and fast.
- ❌ Con: Exchange rate may not be accurate over time.
Alternative: Use historical exchange rates.

### Scale-Up: What to change for 10x larger & hourly data
- **Ditch Batch Overwrites:** Move entirely to PySpark Structured Streaming using Auto Loader to incrementally process hourly micro-batches instead of reading entire directories.

- **Stateful Deduplication within Watermarks:** Window functions over the whole dataset will cause Out-Of-Memory errors at 10x scale. We would switch to dropDuplicatesWithinWatermark() in Structured Streaming to discard late duplicates without keeping infinite historical state.

- **Strict Partition Tuning**: We would definitively partition the Silver layer by event_date (and possibly hour) to limit the shuffle sizes during the downstream Gold aggregation.
