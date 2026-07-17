-- Transaction type table----------
CREATE TABLE IF NOT EXISTS {catalog}.bronze.transaction_type (
    ODS_LOAD_DATETIME TIMESTAMP DEFAULT current_timestamp(),
    SOURCE_NAME STRING,
    TRANSACTION_TYPE_DESCRIPTION STRING,
    ENTITY_FLAG STRING,
    SECURITY_FLAG STRING,
    ASSET_FLAG STRING,
    QUALIFIER_NAME STRING,
    SIGNAGE STRING,
    TRANSACTION_TYPE_CODE_GENERAL_LEDGER STRING
)
USING DELTA
LOCATION 'abfss://{container_location}/PrivateMarket/bronze/transaction_type'
COMMENT 'This table contains transaction type';

--CREATE TABLE SCRIPT FOR ABBREVIATED_NAME--
CREATE TABLE IF NOT EXISTS {catalog}.bronze.abbreviated_name (
    SEQUENCE_ID BIGINT COMMENT 'SEQUENCE_ID',
    ID BIGINT COMMENT 'GENERATED ALWAYS AS IDENTITY COMMENT',
    ORIGINAL_NAME STRING COMMENT 'Original name of transaction type',
    ABBREVIATED_NAME STRING COMMENT 'Abbreviated name of transaction type',
    QUALIFIER STRING COMMENT 'Qualifier of transaction type'
)
USING DELTA
LOCATION 'abfss://{container_location}/PrivateMarket/bronze/abbreviated_name'
COMMENT 'Abbreviated name of transaction type';
