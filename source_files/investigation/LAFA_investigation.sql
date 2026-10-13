/* ============================================================================

   MEASURED 2026-10-12
     HISTORIC DKQ  7,575,258 rows   201,984 samples   ids 1 .. 223,544
     HISTORIC FOS  4,238,860 rows   150,446 samples   ids 1 .. 181,555
     DAILY    DKQ    835,206 rows    18,045 samples   ids 112,914 .. 241,041
     DAILY    FOS    942,451 rows    29,754 samples   ids  93,903 .. 199,870

   ============================================================================ */
SELECT 'HISTORIC' AS table_role, source,
       COUNT(*)                                  AS row_count,
       COUNT(DISTINCT TRIM(id_numeric))          AS distinct_samples,
       MIN(TRY_CAST(TRIM(id_numeric) AS BIGINT)) AS min_id,
       MAX(TRY_CAST(TRIM(id_numeric) AS BIGINT)) AS max_id,
       MAX(last_updated_at)                      AS glue_loaded_at
FROM   c_samp_test_result_lafa
WHERE  regexp_like(TRIM(id_numeric), '^[0-9]+$')
GROUP  BY source
UNION ALL
SELECT 'DAILY', source,
       COUNT(*), COUNT(DISTINCT TRIM(id_numeric)),
       MIN(TRY_CAST(TRIM(id_numeric) AS BIGINT)),
       MAX(TRY_CAST(TRIM(id_numeric) AS BIGINT)),
       MAX(last_updated_at)
FROM   samp_test_result_lafa
WHERE  regexp_like(TRIM(id_numeric), '^[0-9]+$')
GROUP  BY source
ORDER  BY table_role, source;


/* ============================================================================
   Check 1
   The samp_test_result_lafa data is not inside the historic table.
    take every sample in the samp_test_result_lafa, look it up in the
   historic table, and count matches.

   MEASURED 2026-10-12
     source   daily_samples   also_in_historic
     DKQ          18,045            49          
     FOS          29,754             0          
     
   ============================================================================ */
WITH daily AS (
  SELECT DISTINCT source, TRIM(id_numeric) AS sample_id
  FROM   samp_test_result_lafa
), historic AS (
  SELECT DISTINCT source, TRIM(id_numeric) AS sample_id
  FROM   c_samp_test_result_lafa
)
SELECT daily.source,
       COUNT(*)                  AS daily_samples,
       COUNT(historic.sample_id) AS also_in_historic
FROM   daily
LEFT JOIN historic
       ON daily.source    = historic.source
      AND daily.sample_id = historic.sample_id
GROUP  BY daily.source
ORDER  BY daily.source;

/* ============================================================================
   SECTION 4 -- WHAT THE 49 EXCEPTIONS ACTUALLY ARE.
   ----------------------------------------------------------------------------
   MEASURED 2026-10-12
     49 rows returned. For EVERY one, row_delta = 0 (identical row counts on
     both sides). All logged 08/09/10-NOV-2024, plus one on 26-NOV-2024.
     Their ids are 222,990-223,047 and 223,544.
     Note 223,544 is exactly the historic table MAX id from Section 0.

   CONCLUSION: these are samples the source has already inserted into the
   historic table but has not yet deleted from the samp_test_result_lafa. They are the
   moving edge of the archive process. Section 9c proves they arrived in the
   last few days, not in 2024.
   ============================================================================ */
WITH l AS (
  SELECT source, TRY_CAST(TRIM(id_numeric) AS BIGINT) AS id,
         MIN(TRIM(id_text))    AS id_text,
         MIN(TRIM(status))     AS status,
         MIN(TRIM(login_date)) AS login_date,
         COUNT(*)              AS daily_rows
  FROM   samp_test_result_lafa
  WHERE  regexp_like(TRIM(id_numeric), '^[0-9]+$')
  GROUP  BY source, TRY_CAST(TRIM(id_numeric) AS BIGINT)
), h AS (
  SELECT source, TRY_CAST(TRIM(id_numeric) AS BIGINT) AS id,
         MIN(TRIM(status))     AS status,
         MIN(TRIM(login_date)) AS login_date,
         COUNT(*)              AS hist_rows
  FROM   c_samp_test_result_lafa
  WHERE  regexp_like(TRIM(id_numeric), '^[0-9]+$')
  GROUP  BY source, TRY_CAST(TRIM(id_numeric) AS BIGINT)
)
SELECT l.source, l.id, l.id_text,
       l.login_date AS daily_login,  h.login_date AS hist_login,
       l.status     AS daily_status, h.status     AS hist_status,
       l.daily_rows, h.hist_rows,
       l.daily_rows - h.hist_rows AS row_delta
FROM   l
JOIN   h ON l.source = h.source AND l.id = h.id
ORDER  BY l.id;



/* ============================================================================
   Check 4 -- HOW OLD IS THE HISTORIC DATA?
   ----------------------------------------------------------------------------
   WARNING -- READ THIS BEFORE TRUSTING ANY DATE QUERY ON THESE TABLES:
   login_date is TEXT, and the two sites use DIFFERENT formats:
       DKQ = 'DD-MON-YY'  (9 chars, e.g. 26-NOV-24)
       FOS = 'DD/MM/YY'   (8 chars, e.g. 07/12/21)
   A plain MAX(login_date) is therefore WRONG -- it sorts alphabetically, by day
   number and month NAME. This query normalises both formats to YYYY-MM-DD
   first, so MIN/MAX become truly chronological.

   ALSO NOTE: there is no column anywhere in these 81 columns that records when
   the source archived or exported a row. login_date is the sample business
   date. last_updated_at is OUR OWN Glue load timestamp, set at ingestion
   (lims_ingestion_job.py line 78), so it always reads "today" and tells us
   nothing about the source. The only export-time evidence is outside the rows,
   in the gzip file header -- see Section 9a.

   MEASURED 2026-10-12
     table_role  source  oldest_login  newest_login
     HISTORIC    DKQ     1999-12-08    2024-11-26     <-- about 23 months stale
     HISTORIC    FOS     1999-11-17    2023-06-02     <-- about 40 months stale
     DAILY       DKQ     (recent)      (current)
     DAILY       FOS     (recent)      (current)

   CONCLUSION: the historic tables contain nothing recent. They are a closed
   archive of old samples. That is exactly why they almost never change, and
   why reloading them nightly achieves nothing.
   ============================================================================ */
WITH both AS (
  SELECT 'HISTORIC' AS table_role, source, TRIM(login_date) AS ld
  FROM   c_samp_test_result_lafa
  UNION ALL
  SELECT 'DAILY', source, TRIM(login_date)
  FROM   samp_test_result_lafa
), parsed AS (
  SELECT table_role, source,
         CASE
           WHEN length(ld) = 9 THEN                        -- DKQ  DD-MON-YY
             (CASE WHEN substr(ld,8,2) > '50' THEN '19' ELSE '20' END) || substr(ld,8,2) || '-' ||
             CASE substr(ld,4,3)
               WHEN 'JAN' THEN '01' WHEN 'FEB' THEN '02' WHEN 'MAR' THEN '03'
               WHEN 'APR' THEN '04' WHEN 'MAY' THEN '05' WHEN 'JUN' THEN '06'
               WHEN 'JUL' THEN '07' WHEN 'AUG' THEN '08' WHEN 'SEP' THEN '09'
               WHEN 'OCT' THEN '10' WHEN 'NOV' THEN '11' WHEN 'DEC' THEN '12'
             END || '-' || substr(ld,1,2)
           WHEN length(ld) = 8 THEN                        -- FOS  DD/MM/YY
             (CASE WHEN substr(ld,7,2) > '50' THEN '19' ELSE '20' END) || substr(ld,7,2) || '-' ||
             substr(ld,4,2) || '-' || substr(ld,1,2)
         END AS iso_login
  FROM   both
)
SELECT table_role, source,
       MIN(iso_login) AS oldest_login,
       MAX(iso_login) AS newest_login
FROM   parsed
WHERE  iso_login IS NOT NULL
GROUP  BY table_role, source
ORDER  BY table_role, source;

/* ============================================================================
   Check 4 -- FILE LEVEL EVIDENCE   (terminal commands, not SQL)
   ----------------------------------------------------------------------------
   These confirm the same conclusions directly on the raw files we receive,
   before they ever reach Athena. Useful because they are independent of our
   own pipeline.

   FIRST, A FORMAT NOTE THAT CAUSED CONFUSION
     The files arrive named "*.csv" but they are GZIP, not text (magic bytes
     1f 8b). That is why they cannot be opened in Excel or Notepad -- it looks
     like an encoding error but it is a format error. Renaming them to
     "*.csv.gz" resolves it. Underneath, the text is windows-1252 (not UTF-8),
     which is what the Glue config already declares.

   ---------------------------------------------------------------------------
   9a. THE HISTORIC FILE DOES NOT CHANGE FROM DAY TO DAY
   ---------------------------------------------------------------------------
     Three consecutive FOS historic files (received 7, 8 and 9 Oct 2026) are
     byte for byte identical in content.

     Their SHA-256 hashes all differ, which looks like a change -- but that is
     only the gzip header timestamp in bytes 5-8. Skip those and compare the
     actual payload:

         cmp <(tail -c +9 "fileA") <(tail -c +9 "fileB")
         # no output = identical

     RESULT: all three payloads identical. Three days, zero change.

     The gzip header also tells us when the source produced each file:
         file received  7 Oct  ->  gzip timestamp 2026-10-06 22:47 UTC
         file received  8 Oct  ->  gzip timestamp 2026-10-07 22:46 UTC
         file received  9 Oct  ->  gzip timestamp 2026-10-08 22:46 UTC
     (local time +2 = just after midnight each night). Read it with:
         od -An -tu1 -j4 -N4 "file"     # 4 bytes, little endian unix time

   ---------------------------------------------------------------------------
   9b. ONE EXTRACT WE RECEIVED WAS SILENTLY TRUNCATED   *** MAIN RISK ***
   ---------------------------------------------------------------------------
     DKQ historic file of 2 Oct 2026 : 3,171,702 rows | ids stop at 128,533
                                       | newest login only 2016-06-10
     DKQ historic file of 3 Oct 2026 : 7,567,238 rows | ids reach 222,948
                                       | newest login 2024-11-06

     The 2 Oct file ENDS WITH AN ORACLE ERROR written into the data itself:

         ERROR:
         ORA-12571: TNS:packet writer failure

     The Oracle session died mid-export and SQL*Plus wrote the error into the
     output file.

     Why this is dangerous: the file is still VALID GZIP and still parses as
     CSV, so nothing downstream rejects it. Our loader does DELETE + INSERT, so
     loading it would have replaced 7.5M good rows with 3.2M -- silently losing
     95,006 samples and everything after mid-2016.

     Proof it was truncation and not a real data change: all 106,800 samples in
     the 2 Oct file are also in the 3 Oct file, and ZERO samples are unique to
     the 2 Oct file. It is a strict subset, cut off cleanly at one point.

     RECOMMENDED FIX: reject any load whose row count or max id is LOWER than
     what the warehouse already holds. An archive table only ever grows, so a
     shrink always means a bad file. Also fail loudly on 'ORA-' or 'ERROR:'
     text in id_numeric instead of filtering it away silently, which is what
     lims-redshift-loader-lambda.py line 535 currently does.

   ---------------------------------------------------------------------------
   9c. THE HISTORIC TABLE DOES STILL GROW, SLOWLY
   ---------------------------------------------------------------------------
     DKQ historic file of 3 Oct 2026 : 7,567,238 rows | 201,806 samples | max id 222,948
     DKQ historic in Athena 12 Oct   : 7,575,258 rows | 201,984 samples | max id 223,544
                                       ----------------------------------------------
                                       +8,020 rows   |   +178 samples  | +596 ids / 9 days

     All 49 duplicate samples from Section 4 have ids ABOVE 222,948, so they
     were NOT in the 3 Oct file. They entered the historic table after 3 Oct
     2026 -- days ago, not in 2024. Their Nov-2024 login_date is the sample
     business date, not the date they were archived.

     THIS IS THE PROOF that the 49 are the live archiving edge: the source
     copies a batch into the historic table and deletes it from the samp_test_result_lafa
     shortly afterwards, and we happened to measure in between.

     CONSEQUENCE: a small overlap like this will reappear at the new edge after
     every archive batch. The UNION in the report view is therefore permanently
     required, not a temporary workaround.
   ============================================================================ */


/* ============================================================================
   SUMMARY FOR THE DECISION
   ----------------------------------------------------------------------------
   1. The daily and historic data do not overlap.  (Sections 1, 2, 3)
      The source MOVES samples between the tables; each sample is in exactly
      one of them. FOS overlap is 0. DKQ overlap is 49 of 18,045 = 0.27%.

   2. The 49 DKQ exceptions are the archive process caught mid-step, they are
      identical on both sides, and the report view already removes them.
      (Sections 4, 5, 9c)  Our reports are correct today.

   3. The historic data is 23 months (DKQ) and 40 months (FOS) old, and barely
      changes -- FOS did not change at all across three consecutive days.
      (Sections 6, 9a)

   => DECISION: stop the nightly full reload of the historic table. Replace it
      with the Section 8 fingerprint and reload only when it actually changes.
      Keep the samp_test_result_lafa on its full reload, because rows LEAVE that table
      when they are archived, so it needs a delete-aware refresh.

   GUARDRAILS THAT MUST COME WITH IT
   A. Do NOT use an id or date watermark -- it would lose 11,462 FOS samples.
      (Section 7)
   B. Keep UNION, never UNION ALL, in vw_lafa_report_base. (Section 5)
   C. Do NOT compare raw file hashes or S3 ETags to detect change -- the gzip
      timestamp changes nightly even when the data does not. (Section 9a)
   D. Add the shrink-guard FIRST. It is independent of all the above and
      protects against a failure we have already observed. (Section 9b)
   ============================================================================ */
