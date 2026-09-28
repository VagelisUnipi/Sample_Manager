-- DESTINATION
-- Destination material / target reference table (destination matiere).
CREATE TABLE DESTINATION (
    IDENTITY        VARCHAR2(50)    NOT NULL,
    DESCRIPTION     VARCHAR2(200),
    CONSTRAINT PK_DESTINATION PRIMARY KEY (IDENTITY)
);
