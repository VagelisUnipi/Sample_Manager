-- SAMPLE
-- Physical sample records. ID_NUMERIC is referenced as the PK by test result tables.
CREATE TABLE SAMPLE (
    ID_NUMERIC      NUMBER(15)      NOT NULL,
    CONSTRAINT PK_SAMPLE PRIMARY KEY (ID_NUMERIC)
);
