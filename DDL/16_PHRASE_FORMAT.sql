-- PHRASE_FORMAT
-- Lookup table for format/presentation phrases.
-- PHRASE_TYPE = 'FORMAT' selects format entries in BO context filters.
CREATE TABLE PHRASE_FORMAT (
    PHRASE_ID       VARCHAR2(50)    NOT NULL,
    PHRASE_TYPE     VARCHAR2(20)    NOT NULL,
    CONSTRAINT PK_PHRASE_FORMAT PRIMARY KEY (PHRASE_ID)
);
