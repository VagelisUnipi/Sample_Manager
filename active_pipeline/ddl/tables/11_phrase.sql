-- PHRASE (Redshift DW typed table)
-- Deploy order 11/15 (forward FK order).
-- Source of truth: DDL/latest_dll_truth/ALL_TABLES_REDSHIFT.sql

CREATE TABLE PHRASE (
    PHRASE_ID       VARCHAR(50)     NOT NULL,
    PHRASE_TYPE     VARCHAR(20)     NOT NULL,
    ORDER_NUM       BIGINT,
    PHRASE_TEXT     VARCHAR(500),
    ICON            VARCHAR(100),
    SOURCE          VARCHAR(10),
    LAST_UPDATED_AT TIMESTAMP,
    CONSTRAINT PK_PHRASE PRIMARY KEY (PHRASE_ID)
);
