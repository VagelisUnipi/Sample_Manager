-- CODE_CONTROLE (Redshift DW typed table)
-- Deploy order 08/15 (forward FK order).
-- Source of truth: DDL/latest_dll_truth/ALL_TABLES_REDSHIFT.sql

CREATE TABLE CODE_CONTROLE (
    "IDENTITY"          VARCHAR(50)     NOT NULL,
    GROUP_ID            VARCHAR(50),
    DESCRIPTION         VARCHAR(200),
    PRODUIT             VARCHAR(50),
    ORIGINE_MATIERE     VARCHAR(50),
    POINT_PRELEVEMENT   VARCHAR(50),
    CODE_MAT_POLAB      VARCHAR(50),
    MODIFIED_ON         VARCHAR(20),
    MODIFIED_BY         VARCHAR(50),
    MODIFIABLE          CHAR(1),
    REMOVEFLAG          CHAR(1),
    SOURCE              VARCHAR(10),
    LAST_UPDATED_AT     TIMESTAMP,
    CONSTRAINT PK_CODE_CONTROLE PRIMARY KEY ("IDENTITY")
);
