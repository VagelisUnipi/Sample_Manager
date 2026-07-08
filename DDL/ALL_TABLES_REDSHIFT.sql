-- ============================================================================
-- SAMPLE MANAGER — ALL TABLES, REDSHIFT-COMPATIBLE DDL (single paste-ready file)
--
-- Converted from the Oracle DDLs in DDL/01..16:
--   * VARCHAR2(n)  -> VARCHAR(n)
--   * NUMBER(p)    -> BIGINT
--   * ANALYSE, IDENTITY, LOCATION are Redshift reserved words -> double-quoted
--   * CHECK constraints are not supported by Redshift -> removed (kept as comments)
--   * FKs that reference only part of a composite PK are invalid in Redshift
--     -> commented out (FK_MENSUEL_VERSIONED_ANALYSIS, FK_MLP_VIEW_VERSIONED_ANALYSIS)
--   * Tables reordered so every referenced table exists before its FK
--
-- NOTE: In Redshift, PRIMARY KEY / FOREIGN KEY are informational only
-- (used by the planner, NOT enforced). Uniqueness must be guaranteed by the load.
--
-- Optional: run inside the target schema first, e.g.
--   CREATE SCHEMA IF NOT EXISTS prism_edhid_samplemanager_dw;
--   SET search_path TO prism_edhid_samplemanager_dw;
-- ============================================================================


-- ----------------------------------------------------------------------------
-- VERSIONED_ANALYSIS
-- Versioned analysis method definitions.
-- Composite PK: IDENTITY + ANALYSIS_VERSION.
-- ----------------------------------------------------------------------------
CREATE TABLE VERSIONED_ANALYSIS (
    "IDENTITY"          VARCHAR(50)     NOT NULL,
    ANALYSIS_VERSION    BIGINT          NOT NULL,
    CONSTRAINT PK_VERSIONED_ANALYSIS PRIMARY KEY ("IDENTITY", ANALYSIS_VERSION)
);

-- ----------------------------------------------------------------------------
-- VERSIONED_COMPONENT
-- Versioned analyte/component definitions linked to a specific analysis version.
-- Composite PK: NAME + ANALYSIS + ANALYSIS_VERSION.
-- ----------------------------------------------------------------------------
CREATE TABLE VERSIONED_COMPONENT (
    NAME                VARCHAR(100)    NOT NULL,
    ANALYSIS            VARCHAR(50)     NOT NULL,
    ANALYSIS_VERSION    BIGINT          NOT NULL,
    CONSTRAINT PK_VERSIONED_COMPONENT PRIMARY KEY (NAME, ANALYSIS, ANALYSIS_VERSION),
    CONSTRAINT FK_VCOMP_VERSIONED_ANALYSIS FOREIGN KEY (ANALYSIS, ANALYSIS_VERSION)
        REFERENCES VERSIONED_ANALYSIS ("IDENTITY", ANALYSIS_VERSION)
);

-- ----------------------------------------------------------------------------
-- MLP_HEADER
-- Product / MLP (Material/Product) header records.
-- Composite PK: IDENTITY + PRODUCT_VERSION.
-- ----------------------------------------------------------------------------
CREATE TABLE MLP_HEADER (
    "IDENTITY"      VARCHAR(50)     NOT NULL,
    PRODUCT_VERSION BIGINT          NOT NULL,
    CONSTRAINT PK_MLP_HEADER PRIMARY KEY ("IDENTITY", PRODUCT_VERSION)
);

-- ----------------------------------------------------------------------------
-- SAMPLE
-- Physical sample records. ID_NUMERIC is referenced as the PK by test result tables.
-- ----------------------------------------------------------------------------
CREATE TABLE SAMPLE (
    ID_NUMERIC      BIGINT          NOT NULL,
    CONSTRAINT PK_SAMPLE PRIMARY KEY (ID_NUMERIC)
);

-- ----------------------------------------------------------------------------
-- SAMPLE_POINT
-- Sampling point / collection point reference table.
-- ----------------------------------------------------------------------------
CREATE TABLE SAMPLE_POINT (
    "IDENTITY"      VARCHAR(50)     NOT NULL,
    CONSTRAINT PK_SAMPLE_POINT PRIMARY KEY ("IDENTITY")
);

-- ----------------------------------------------------------------------------
-- CUSTOMER
-- Customer reference table.
-- ----------------------------------------------------------------------------
CREATE TABLE CUSTOMER (
    "IDENTITY"      VARCHAR(50)     NOT NULL,
    CONSTRAINT PK_CUSTOMER PRIMARY KEY ("IDENTITY")
);

-- ----------------------------------------------------------------------------
-- LOCATION  (reserved word in Redshift -> table name quoted)
-- Physical location / site reference table.
-- ----------------------------------------------------------------------------
CREATE TABLE "LOCATION" (
    "IDENTITY"      VARCHAR(50)     NOT NULL,
    CONSTRAINT PK_LOCATION PRIMARY KEY ("IDENTITY")
);

-- ----------------------------------------------------------------------------
-- CODE_CONTROLE
-- Quality control code reference table.
-- ----------------------------------------------------------------------------
CREATE TABLE CODE_CONTROLE (
    "IDENTITY"      VARCHAR(50)     NOT NULL,
    CONSTRAINT PK_CODE_CONTROLE PRIMARY KEY ("IDENTITY")
);

-- ----------------------------------------------------------------------------
-- FOURNISSEUR
-- Supplier (fournisseur) reference table.
-- ----------------------------------------------------------------------------
CREATE TABLE FOURNISSEUR (
    "IDENTITY"      VARCHAR(50)     NOT NULL,
    CONSTRAINT PK_FOURNISSEUR PRIMARY KEY ("IDENTITY")
);

-- ----------------------------------------------------------------------------
-- JOB_HEADER
-- Laboratory job / batch header reference table.
-- ----------------------------------------------------------------------------
CREATE TABLE JOB_HEADER (
    JOB_NAME        VARCHAR(100)    NOT NULL,
    CONSTRAINT PK_JOB_HEADER PRIMARY KEY (JOB_NAME)
);

-- ----------------------------------------------------------------------------
-- PHRASE
-- Lookup table for coded phrases/labels (e.g. granulometry descriptions).
-- PHRASE_TYPE = 'GRANULO' selects granulometry entries in BO context filters.
-- ----------------------------------------------------------------------------
CREATE TABLE PHRASE (
    PHRASE_ID       VARCHAR(50)     NOT NULL,
    PHRASE_TYPE     VARCHAR(20)     NOT NULL,
    CONSTRAINT PK_PHRASE PRIMARY KEY (PHRASE_ID)
);

-- ----------------------------------------------------------------------------
-- DESTINATION
-- Destination material / target reference table (destination matiere).
-- ----------------------------------------------------------------------------
CREATE TABLE DESTINATION (
    "IDENTITY"      VARCHAR(50)     NOT NULL,
    CONSTRAINT PK_DESTINATION PRIMARY KEY ("IDENTITY")
);

-- ----------------------------------------------------------------------------
-- PHRASE_FORMAT
-- Lookup table for format/presentation phrases.
-- PHRASE_TYPE = 'FORMAT' selects format entries in BO context filters.
-- ----------------------------------------------------------------------------
CREATE TABLE PHRASE_FORMAT (
    PHRASE_ID       VARCHAR(50)     NOT NULL,
    PHRASE_TYPE     VARCHAR(20)     NOT NULL,
    CONSTRAINT PK_PHRASE_FORMAT PRIMARY KEY (PHRASE_ID)
);

-- ----------------------------------------------------------------------------
-- MLP_VIEW
-- Material/product limit plan view: links analysis, component, and product with their limits.
-- Joined with outer joins, so acts as an optional enrichment table.
-- ----------------------------------------------------------------------------
CREATE TABLE MLP_VIEW (
    ANALYSIS_ID     VARCHAR(50)     NOT NULL,
    COMPONENT_NAME  VARCHAR(100)    NOT NULL,
    PRODUCT_ID      VARCHAR(50)     NOT NULL,
    PRODUCT_VERSION BIGINT          NOT NULL,
    CONSTRAINT PK_MLP_VIEW PRIMARY KEY (ANALYSIS_ID, COMPONENT_NAME, PRODUCT_ID, PRODUCT_VERSION),
    -- Not valid in Redshift: references only part of VERSIONED_ANALYSIS composite PK
    -- CONSTRAINT FK_MLP_VIEW_VERSIONED_ANALYSIS FOREIGN KEY (ANALYSIS_ID)
    --     REFERENCES VERSIONED_ANALYSIS ("IDENTITY"),
    CONSTRAINT FK_MLP_VIEW_MLP_HEADER FOREIGN KEY (PRODUCT_ID, PRODUCT_VERSION)
        REFERENCES MLP_HEADER ("IDENTITY", PRODUCT_VERSION)
);

-- ----------------------------------------------------------------------------
-- MENSUEL
-- Monthly aggregated results by product, analysis, and control code.
-- ANNUEL = 'F' distinguishes monthly rows from annual (ANNUEL table alias).
-- ----------------------------------------------------------------------------
CREATE TABLE MENSUEL (
    "ANALYSE"       VARCHAR(50)     NOT NULL,
    ANNUEL          CHAR(1)         NOT NULL,   -- 'F' = monthly
    MESURE          VARCHAR(100)    NOT NULL,
    PRODUIT         VARCHAR(50)     NOT NULL,
    PRODUCT_VERSION BIGINT          NOT NULL,
    LOCALISATION    VARCHAR(50)     NOT NULL,
    CODE_CONTROLE   VARCHAR(50),
    CONSTRAINT PK_MENSUEL PRIMARY KEY ("ANALYSE", PRODUIT, PRODUCT_VERSION, MESURE, LOCALISATION),
    -- Not valid in Redshift: references only part of VERSIONED_ANALYSIS composite PK
    -- CONSTRAINT FK_MENSUEL_VERSIONED_ANALYSIS FOREIGN KEY ("ANALYSE")
    --     REFERENCES VERSIONED_ANALYSIS ("IDENTITY"),
    CONSTRAINT FK_MENSUEL_LOCATION FOREIGN KEY (LOCALISATION)
        REFERENCES "LOCATION" ("IDENTITY"),
    CONSTRAINT FK_MENSUEL_CODE_CONTROLE FOREIGN KEY (CODE_CONTROLE)
        REFERENCES CODE_CONTROLE ("IDENTITY")
    -- CHECK not supported in Redshift: CHK_MENSUEL_ANNUEL CHECK (ANNUEL = 'F')
);

-- ----------------------------------------------------------------------------
-- SAMP_TEST_RESULT_LAFA
-- Core fact table: laboratory test results at detail level (rolling 2-year window).
-- STATUS / RESULT_STATUS / TEST_STATUS = 'X' marks excluded/cancelled records.
-- ----------------------------------------------------------------------------
CREATE TABLE SAMP_TEST_RESULT_LAFA (
    -- Analysis identification
    ANALYSIS            VARCHAR(50)     NOT NULL,
    ANALYSIS_VERSION    BIGINT          NOT NULL,
    COMPONENT_NAME      VARCHAR(100)    NOT NULL,
    PRODUCT             VARCHAR(50)     NOT NULL,
    PRODUCT_VERSION     BIGINT          NOT NULL,

    -- Sampling context
    SAMPLING_POINT      VARCHAR(50),
    LOCATION_ID         VARCHAR(50),
    DESTINATION_MATIERE VARCHAR(50),

    -- Classification FKs
    CODE_CONTROLE       VARCHAR(50),
    CUSTOMER_ID         VARCHAR(50)     NOT NULL,
    FOURNISSEUR         VARCHAR(50)     NOT NULL,

    -- Status flags ('X' = excluded/cancelled)
    STATUS              CHAR(1),
    RESULT_STATUS       CHAR(1),
    TEST_STATUS         CHAR(1),

    -- Links to source sample and job
    ORIGINAL_SAMPLE     BIGINT,
    JOB_NAME            VARCHAR(100),

    -- Lookup FKs for descriptive attributes
    GRANULOMETRIE       VARCHAR(50),
    FORMAT              VARCHAR(50),
    TEST_SCHEDULE       VARCHAR(50),

    CONSTRAINT FK_STRL_VERSIONED_ANALYSIS FOREIGN KEY (ANALYSIS, ANALYSIS_VERSION)
        REFERENCES VERSIONED_ANALYSIS ("IDENTITY", ANALYSIS_VERSION),
    CONSTRAINT FK_STRL_VERSIONED_COMPONENT FOREIGN KEY (COMPONENT_NAME, ANALYSIS, ANALYSIS_VERSION)
        REFERENCES VERSIONED_COMPONENT (NAME, ANALYSIS, ANALYSIS_VERSION),
    CONSTRAINT FK_STRL_MLP_HEADER FOREIGN KEY (PRODUCT, PRODUCT_VERSION)
        REFERENCES MLP_HEADER ("IDENTITY", PRODUCT_VERSION),
    CONSTRAINT FK_STRL_SAMPLE_POINT FOREIGN KEY (SAMPLING_POINT)
        REFERENCES SAMPLE_POINT ("IDENTITY"),
    CONSTRAINT FK_STRL_LOCATION FOREIGN KEY (LOCATION_ID)
        REFERENCES "LOCATION" ("IDENTITY"),
    CONSTRAINT FK_STRL_DESTINATION FOREIGN KEY (DESTINATION_MATIERE)
        REFERENCES DESTINATION ("IDENTITY"),
    CONSTRAINT FK_STRL_CODE_CONTROLE FOREIGN KEY (CODE_CONTROLE)
        REFERENCES CODE_CONTROLE ("IDENTITY"),
    CONSTRAINT FK_STRL_CUSTOMER FOREIGN KEY (CUSTOMER_ID)
        REFERENCES CUSTOMER ("IDENTITY"),
    CONSTRAINT FK_STRL_FOURNISSEUR FOREIGN KEY (FOURNISSEUR)
        REFERENCES FOURNISSEUR ("IDENTITY"),
    CONSTRAINT FK_STRL_SAMPLE FOREIGN KEY (ORIGINAL_SAMPLE)
        REFERENCES SAMPLE (ID_NUMERIC),
    CONSTRAINT FK_STRL_JOB_HEADER FOREIGN KEY (JOB_NAME)
        REFERENCES JOB_HEADER (JOB_NAME),
    CONSTRAINT FK_STRL_PHRASE FOREIGN KEY (GRANULOMETRIE)
        REFERENCES PHRASE (PHRASE_ID),
    CONSTRAINT FK_STRL_PHRASE_FORMAT FOREIGN KEY (FORMAT)
        REFERENCES PHRASE_FORMAT (PHRASE_ID)
);
