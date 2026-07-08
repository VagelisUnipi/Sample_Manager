-- PHRASE
-- Lookup table for coded phrases/labels (e.g. granulometry descriptions).
-- PHRASE_TYPE = 'GRANULO' selects granulometry entries in BO context filters.
CREATE TABLE PHRASE (
    PHRASE_ID       VARCHAR2(50)    NOT NULL,
    PHRASE_TYPE     VARCHAR2(20)    NOT NULL,
    CONSTRAINT PK_PHRASE PRIMARY KEY (PHRASE_ID)
);
