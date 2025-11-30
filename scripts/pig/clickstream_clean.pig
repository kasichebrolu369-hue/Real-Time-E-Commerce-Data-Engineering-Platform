/*
 * Clickstream Data Cleaning - Pig ETL Script
 * ===========================================
 * 
 * PURPOSE:
 * This Pig Latin script cleans and transforms raw clickstream data from JSON format.
 * 
 * OPERATIONS:
 * 1. Load raw clickstream JSON data
 * 2. Parse and validate JSON fields
 * 3. Remove invalid/malformed records
 * 4. Filter bot traffic and suspicious IPs
 * 5. Normalize timestamps
 * 6. Remove duplicate events
 * 7. Store cleaned data in Parquet format
 * 
 * INPUT: /ecommerce/raw/clickstream/YYYY/MM/DD/*.json
 * OUTPUT: /ecommerce/clean/clickstream/
 * 
 * USAGE:
 * pig -param input_path=/ecommerce/raw/clickstream/2025/11/30 \
 *     -param output_path=/ecommerce/clean/clickstream/2025/11/30 \
 *     clickstream_clean.pig
 */

-- ============================================================================
-- STEP 1: SET CONFIGURATION
-- ============================================================================

-- Enable compression
SET output.compression.enabled true;
SET output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

-- Set parallelism for better performance
SET default_parallel 20;

-- Register JSON parsing library (if needed)
-- REGISTER /path/to/json-simple-1.1.jar;
-- REGISTER /path/to/elephant-bird.jar;

DEFINE JsonLoader org.apache.pig.piggybank.storage.JsonLoader();

-- ============================================================================
-- STEP 2: LOAD RAW CLICKSTREAM DATA
-- ============================================================================

-- Load raw clickstream events from JSON
raw_clickstream = LOAD '$input_path/*.json' 
    USING JsonLoader(
        'event_id:chararray,
         user_id:chararray,
         session_id:chararray,
         event_time:chararray,
         page_url:chararray,
         product_id:chararray,
         event_type:chararray,
         device_type:chararray,
         ip_address:chararray,
         traffic_source:chararray,
         referrer:chararray,
         user_agent:chararray,
         country:chararray,
         city:chararray,
         browser:chararray,
         os:chararray'
    );

-- Display sample data for debugging
-- DUMP raw_clickstream;
-- DESCRIBE raw_clickstream;

-- ============================================================================
-- STEP 3: DATA VALIDATION AND FILTERING
-- ============================================================================

-- Filter out records with NULL critical fields
valid_clickstream = FILTER raw_clickstream BY 
    event_id IS NOT NULL AND
    user_id IS NOT NULL AND
    session_id IS NOT NULL AND
    event_time IS NOT NULL AND
    event_type IS NOT NULL;

-- Count valid records
valid_count = GROUP valid_clickstream ALL;
valid_stats = FOREACH valid_count GENERATE COUNT(valid_clickstream) AS valid_count;

-- ============================================================================
-- STEP 4: REMOVE BOT TRAFFIC
-- ============================================================================

-- Filter out common bot user agents
non_bot_clickstream = FILTER valid_clickstream BY 
    NOT (
        user_agent MATCHES '.*[Bb]ot.*' OR
        user_agent MATCHES '.*[Ss]pider.*' OR
        user_agent MATCHES '.*[Cc]rawler.*' OR
        user_agent MATCHES '.*[Ss]craper.*' OR
        user_agent MATCHES '.*curl.*' OR
        user_agent MATCHES '.*wget.*' OR
        user_agent MATCHES '.*python.*'
    );

-- ============================================================================
-- STEP 5: DATA ENRICHMENT AND TRANSFORMATION
-- ============================================================================

-- Parse and normalize timestamps
-- Convert event_time string to proper timestamp format
enriched_clickstream = FOREACH non_bot_clickstream GENERATE
    event_id,
    user_id,
    session_id,
    ToDate(event_time, 'yyyy-MM-dd HH:mm:ss') AS event_timestamp,
    page_url,
    product_id,
    event_type,
    device_type,
    ip_address,
    traffic_source,
    referrer,
    user_agent,
    country,
    city,
    browser,
    os,
    -- Extract date components for partitioning
    ToString(ToDate(event_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS event_date,
    GetYear(ToDate(event_time, 'yyyy-MM-dd HH:mm:ss')) AS year,
    GetMonth(ToDate(event_time, 'yyyy-MM-dd HH:mm:ss')) AS month,
    GetDay(ToDate(event_time, 'yyyy-MM-dd HH:mm:ss')) AS day,
    GetHour(ToDate(event_time, 'yyyy-MM-dd HH:mm:ss')) AS hour;

-- ============================================================================
-- STEP 6: REMOVE DUPLICATES
-- ============================================================================

-- Group by event_id to identify duplicates
grouped_by_event = GROUP enriched_clickstream BY event_id;

-- Keep only the first occurrence of each event_id
deduped_clickstream = FOREACH grouped_by_event {
    sorted = ORDER enriched_clickstream BY event_timestamp DESC;
    latest = LIMIT sorted 1;
    GENERATE FLATTEN(latest);
}

-- ============================================================================
-- STEP 7: DATA QUALITY CHECKS
-- ============================================================================

-- Filter out records with invalid event types
valid_event_types = FILTER deduped_clickstream BY 
    event_type IN ('view', 'add_to_cart', 'remove_from_cart', 'purchase', 'exit');

-- Filter out records with invalid device types
clean_clickstream = FILTER valid_event_types BY 
    device_type IN ('mobile', 'desktop', 'tablet');

-- ============================================================================
-- STEP 8: COMPUTE STATISTICS (OPTIONAL)
-- ============================================================================

-- Count by event type
event_type_stats = GROUP clean_clickstream BY event_type;
event_counts = FOREACH event_type_stats GENERATE 
    group AS event_type, 
    COUNT(clean_clickstream) AS count;

STORE event_counts INTO '$output_path/stats/event_type_counts' 
    USING PigStorage(',');

-- Count by device type
device_stats = GROUP clean_clickstream BY device_type;
device_counts = FOREACH device_stats GENERATE 
    group AS device_type, 
    COUNT(clean_clickstream) AS count;

STORE device_counts INTO '$output_path/stats/device_counts' 
    USING PigStorage(',');

-- Daily event counts
daily_stats = GROUP clean_clickstream BY event_date;
daily_counts = FOREACH daily_stats GENERATE 
    group AS date, 
    COUNT(clean_clickstream) AS total_events;

STORE daily_counts INTO '$output_path/stats/daily_counts' 
    USING PigStorage(',');

-- ============================================================================
-- STEP 9: SAVE CLEANED DATA
-- ============================================================================

-- Store cleaned clickstream data in Parquet format with partitioning
STORE clean_clickstream INTO '$output_path/data' 
    USING org.apache.parquet.pig.ParquetStorer(
        'event_id:chararray,
         user_id:chararray,
         session_id:chararray,
         event_timestamp:datetime,
         page_url:chararray,
         product_id:chararray,
         event_type:chararray,
         device_type:chararray,
         ip_address:chararray,
         traffic_source:chararray,
         referrer:chararray,
         user_agent:chararray,
         country:chararray,
         city:chararray,
         browser:chararray,
         os:chararray,
         event_date:chararray,
         year:int,
         month:int,
         day:int,
         hour:int'
    );

-- ============================================================================
-- STEP 10: QUALITY REPORT
-- ============================================================================

-- Generate quality metrics
quality_metrics = GROUP clean_clickstream ALL;
quality_report = FOREACH quality_metrics GENERATE 
    COUNT(clean_clickstream) AS total_clean_records,
    COUNT_STAR(clean_clickstream) AS total_rows;

STORE quality_report INTO '$output_path/quality_report' 
    USING PigStorage(',');

/*
 * ============================================================================
 * EXECUTION NOTES:
 * ============================================================================
 * 
 * 1. BEFORE RUNNING:
 *    - Ensure raw clickstream data is available in HDFS
 *    - Check that output directory doesn't exist (or remove it)
 *    - Verify JAR files for JSON parsing are available
 * 
 * 2. RUNNING THE SCRIPT:
 *    
 *    Local mode (for testing):
 *    pig -x local -param input_path=../data/raw/clickstream/2025/11/30 \
 *        -param output_path=../data/clean/clickstream/2025/11/30 \
 *        clickstream_clean.pig
 *    
 *    MapReduce mode (production):
 *    pig -x mapreduce -param input_path=/ecommerce/raw/clickstream/2025/11/30 \
 *        -param output_path=/ecommerce/clean/clickstream/2025/11/30 \
 *        clickstream_clean.pig
 * 
 * 3. MONITORING:
 *    - Check Hadoop job tracker for progress
 *    - Monitor HDFS space usage
 *    - Review quality_report for data quality metrics
 * 
 * 4. OUTPUT STRUCTURE:
 *    /ecommerce/clean/clickstream/YYYY/MM/DD/
 *        ├── data/          (cleaned Parquet files)
 *        ├── stats/         (aggregated statistics)
 *        └── quality_report/ (data quality metrics)
 * 
 * 5. PERFORMANCE TUNING:
 *    - Adjust default_parallel based on cluster size
 *    - Enable compression to reduce storage
 *    - Use appropriate file sizes (128MB - 256MB blocks)
 * 
 * ============================================================================
 */
