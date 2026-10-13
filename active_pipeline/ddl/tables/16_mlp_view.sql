-- MLP_VIEW (Redshift DW typed table)
-- Deploy order 16/16 (forward FK order).
-- Material limit plan: the spec limits per product / analysis / component / level.
--
-- NOTE ON SCOPE: this table extends DDL/latest_dll_truth/ALL_TABLES_REDSHIFT.sql.
-- The truth file declares 10 columns; the real MLP_VIEW_prod_extract delivers 23.
-- All 10 truth columns are present and keep their truth types/sizes. The 13
-- additional columns below were sized from the 20,446-row extract of 2026-10-11
-- (max observed length shown in each comment, with headroom).
-- Column order mirrors the extract header, matching the house style used by the
-- SAMP_TEST_RESULT_LAFA tables.
--
-- PRIMARY KEY CORRECTION (important):
--   The truth file declares PK (ANALYSIS_ID, COMPONENT_NAME, PRODUCT_ID,
--   PRODUCT_VERSION). That key DOES NOT HOLD in the real data: 20,446 rows
--   collapse to only 13,926 distinct values on it (6,520 duplicates), because a
--   single product/analysis/component carries MULTIPLE spec levels (LEVEL_ID has
--   17 distinct values: ACHAT, ALARME, ZONEGRISE, ...).
--   Adding LEVEL_ID makes it exact: 20,446 distinct / 20,446 rows, 0 duplicates.
--   This matters beyond documentation -- Redshift does not enforce constraints
--   but the query planner TRUSTS them, so declaring a non-unique key as PRIMARY
--   KEY can produce incorrect results (eliminated joins, skipped de-duplication).

CREATE TABLE MLP_VIEW (
    PRODUCT_ID          VARCHAR(50)     NOT NULL,   -- business product name, e.g. TRG, 1SILVER (max 10)
    PRODUCT_VERSION     BIGINT          NOT NULL,
    ANALYSIS_ID         VARCHAR(50)     NOT NULL,   -- max 10; 69 rows carry the literal '$NULL'
    COMPONENT_NAME      VARCHAR(100)    NOT NULL,   -- max 37
    COMPONENT_TYPE      CHAR(1),                    -- N | T
    LEVEL_ID            VARCHAR(50)     NOT NULL,   -- part of the real PK; 17 distinct, never blank
    LEVEL_ORDER         VARCHAR(50),                -- truth type kept; data is a single digit 1-6
    CONTINUE_CHECKING   CHAR(1),                    -- N | Y
    MESSAGE_ACTION      CHAR(1),                    -- F | T
    REPORT_ACTION       CHAR(1),                    -- D
    RESULT_ACTION       CHAR(1),                    -- A | D
    DISPLAY_MESSAGE     VARCHAR(500),               -- max 108, contains accented French
    REPORT_NAME         VARCHAR(50),                -- max 4; blank on 19,939 of 20,446 rows
    ROUTINE_NAME        VARCHAR(50),                -- blank on every row in the 2026-10-11 extract
    MIN_LIMIT           VARCHAR(50),                -- 1,108 distinct; blank on 98 rows
    MAX_LIMIT           VARCHAR(50),                -- 924 distinct; blank on 4,755 rows
    TEXT_SPEC           VARCHAR(500),               -- blank on every row in the 2026-10-11 extract
    TEXT_PHRASE         VARCHAR(50),                -- max 10; blank on 20,406 rows
    BOOLEAN_PASS        CHAR(1),                    -- F
    TYPICAL_TEXT        VARCHAR(50),                -- 180 distinct; blank on 18,070 rows
    PRODUCT_CODE        VARCHAR(50),                -- max 12; blank on 13,656 rows. NOT a key to MLP_HEADER (see below)
    DUREE_ARCHI         VARCHAR(50),                -- max 16, interval-as-text e.g. '0 00:00:00.00'
    DESCRIPTION         VARCHAR(200),               -- max 78, contains accented French
    SOURCE              VARCHAR(10),                -- pipeline-added: DKQ | FOS (ext partition column)
    LAST_UPDATED_AT     TIMESTAMP,                  -- pipeline-added: Glue run timestamp
    CONSTRAINT PK_MLP_VIEW PRIMARY KEY
        (ANALYSIS_ID, COMPONENT_NAME, PRODUCT_ID, PRODUCT_VERSION, LEVEL_ID)

    -- FOREIGN KEYS DELIBERATELY OMITTED -- both fail against the real extract,
    -- and a false constraint misleads the Redshift planner exactly like a false
    -- PK does. Re-enable only once the underlying key problems are fixed.
    --
    -- , CONSTRAINT FK_MLP_VIEW_MLP_HEADER FOREIGN KEY (PRODUCT_ID, PRODUCT_VERSION)
    --       REFERENCES MLP_HEADER ("IDENTITY", PRODUCT_VERSION)
    --   BROKEN: 0 of 252 MLP_VIEW.PRODUCT_ID values match MLP_HEADER."IDENTITY".
    --   The loader maps MLP_HEADER."IDENTITY" from the extract column ENTRY_CODE
    --   (lims-redshift-loader-lambda.py:173), but ENTRY_CODE holds numeric
    --   surrogates ('0','100','10020','9995') while MLP_VIEW.PRODUCT_ID holds
    --   business names ('TRG','1SILVER','TERNALEVO'). Two different identifier
    --   spaces. This is the same root cause as the already-known broken
    --   mlp_header join in vw_lafa_report_base (product_label always falls back
    --   to its placeholder, product_active always evaluates to 'NON').
    --   MLP_VIEW.PRODUCT_CODE does NOT bridge them either (0 of 86 match).
    --
    -- , CONSTRAINT FK_MLP_VIEW_VERSIONED_ANALYSIS FOREIGN KEY (ANALYSIS_ID)
    --       REFERENCES VERSIONED_ANALYSIS ("IDENTITY")
    --   PARTIAL: 44 of 46 ANALYSIS_ID values match VERSIONED_ANALYSIS."IDENTITY"
    --   (95.7%). The orphans include the literal '$NULL' sentinel (69 rows), so
    --   the constraint would be violated.
);

-- ---------------------------------------------------------------------------
-- WHAT THIS TABLE UNBLOCKS
-- ---------------------------------------------------------------------------
-- vw_lafa_report_base currently hardcodes four spec columns as literals,
-- identical on every row, because this table was never ingested
-- (see docs/vw_lafa_report_base_hardcoded_fields.txt):
--
--     spec_level    'ECART_MP'   ->  mlp_view.level_id      (17 real values)
--     target_value  '52.5'       ->  mlp_view.typical_text  (180 real values)
--     spec_min      '54.50'      ->  mlp_view.min_limit     (1,108 real values)
--     spec_max      '100.00'     ->  mlp_view.max_limit     (924 real values)
--
-- The commented join at vw_lafa_report_base.sql:65 and :127 keys on
-- (analysis_id, component_name, product_id, product_version) and WILL FAN OUT
-- the report if enabled as written -- that is the non-unique key above. It needs
-- either LEVEL_ID added to the join, or a rule selecting ONE level per row
-- (e.g. a fixed LEVEL_ID, or MIN(LEVEL_ORDER)). That is a reporting decision:
-- it determines which spec limits every row displays.
--
-- The join itself is otherwise sound -- verified against the FOS LAFA fact table:
--     210 of 252 MLP_VIEW.PRODUCT_ID found in LAFA.PRODUCT            (83.3%)
--     678 of 833 (PRODUCT_ID, PRODUCT_VERSION) pairs matched          (81.4%)
-- i.e. mlp_view.product_id joins to samp_test_result_lafa.product correctly,
-- unlike the mlp_header path.
--
-- Note TYPICAL_TEXT is blank on 88% of rows and MAX_LIMIT on 23%, so
-- target_value and spec_max will be NULL for most rows once wired to real data.
-- Wrap them in COALESCE if the report must keep showing a value.
-- ---------------------------------------------------------------------------
