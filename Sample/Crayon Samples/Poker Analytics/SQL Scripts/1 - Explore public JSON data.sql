/***** USE THE SCRIPTS BELOW TO QUERY PUBLIC JSON FILES CONTAINING ONLINE POKER LOG INFORMATION *****

1 - QUERY RAW JSON
2 - PARSE JSON USING T-SQL FUNCTIONS
3 - PERSIST AS SQL OBJECTS
4 - ANALYTICAL QUERIES
5 - USE CETAS TO PERSIST STRUCTURED RESULTS TO PARQUET
6 - CREATE SQL VIEWS TO EXPOSE NEWLY CREATED PARQUET DATA

[Convert GameDetails to Delta] Notebook can be executed once queries below have been run successfully

*****/

--> 1 - Query files using OPENROWSET 
SELECT  TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://alexmodernanalytics01.dfs.core.windows.net/src/Poker Data/PokerJSONTableData/JSON/**',
        FORMAT = 'CSV',
        FIELDQUOTE = '0x0b',
        FIELDTERMINATOR ='0x0b'--,
        --ROWTERMINATOR = '0x0b'
        )
    WITH (
        jsonContent varchar(MAX)
    ) AS [result]
;

--> 2 - Parse documents returned using json_value and json_query
SELECT TOP 100 JSON_VALUE(rd.jsoncontent, '$.id') GameId
            , JSON_VALUE(rd.jsoncontent, '$.tablename') TableName
            , JSON_VALUE(rd.jsoncontent, '$.timestamp') as GameTime
            , JSON_VALUE(rd.jsoncontent, '$.actions') as Actions
            , JSON_VALUE(rd.jsoncontent, '$.board') as Board
FROM
    OPENROWSET(
        BULK 'https://alexmodernanalytics01.dfs.core.windows.net/src/Poker Data/PokerJSONTableData/JSON/**',
        FORMAT = 'CSV',
        FIELDQUOTE = '0x0b',
        FIELDTERMINATOR ='0x0b'--,
        --ROWTERMINATOR = '0x0b'
        )
    WITH (
        jsonContent varchar(MAX)
    ) AS rd
;


--> 3 - Persist as SQL Objects

--Check for the existence of the database Pokerdata in SQL Serverless pool and create it if not found
IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'Pokerdata')
BEGIN
    CREATE DATABASE Pokerdata;
END
GO

--Change database context to 'Pokerdata'
USE Pokerdata
GO

--Drop and create the external data source that can be used to store file location for OPENROWSET queries
IF EXISTS(SELECT * FROM sys.external_data_sources WHERE name = 'pokerdata')
    DROP EXTERNAL DATA SOURCE  pokerdata;
GO

CREATE EXTERNAL DATA SOURCE pokerdata
    WITH
    (
        LOCATION = 'https://alexmodernanalytics01.dfs.core.windows.net/src/Poker Data/PokerJSONTableData/JSON'
    );

GO

--Persist JSON docs as SQL View dbo.RawData
DROP VIEW IF EXISTS dbo.RawData;
GO

CREATE VIEW dbo.RawData
AS
SELECT  *
FROM
    OPENROWSET(
        BULK '/**',
        FORMAT = 'CSV',
        FIELDQUOTE = '0x0b',
        FIELDTERMINATOR ='0x0b'--,
        --ROWTERMINATOR = '0x0b'
        , DATA_SOURCE = 'pokerdata'
    )
    WITH (
        jsonContent varchar(MAX)
    ) AS [result]
;
GO

/* TEST RESULTS

SELECT top 100 *
from dbo.RawData

*/


--Drop and create the view dbo.GameDetails to expose parsed JSON entities
DROP VIEW IF EXISTS dbo.GameDetails;
GO

CREATE VIEW dbo.GameDetails (GameId, TableName, Board, GameTime, GameActionId, Player, StreetName, Action, Amount, Cards, StreetSort) 
AS 
WITH L1 AS (
    select  json_value(rd.jsoncontent, '$.id') GameId
            , json_value(rd.jsoncontent, '$.tablename') TableName
            , json_value(rd.jsoncontent, '$.timestamp') as GameTime
            , json_query(rd.jsoncontent, '$.actions') as Actions
            , json_value(rd.jsoncontent, '$.board') as Board
    from dbo.RawData rd
), L2 AS (
select L1.GameId
    , L1.TableName
    , L1.Board
    , cast(replace(L1.GameTime, ' ET', '') as datetime2) GameTime
    , cast(json_value(j.value, '$.GameActionId') as bigint) GameActionId
    , json_value(j.value, '$.actor') Player
    , json_value(j.value, '$.streetname') StreetName
    , json_value(j.value, '$.action') Action
    , cast(json_value(j.value, '$.amount') as decimal(9,2)) Amount
    , json_value(j.value, '$.cards') as Cards
from L1
CROSS APPLY OPENJSON(L1.Actions) j
)
SELECT GameId
        , TableName
        , Board
        , GameTime
        , GameActionId
        , Player
        , StreetName
        , Action
        , Amount
        , Cards
        , CASE StreetName
            WHEN 'Pre-Flop' THEN 1
            WHEN 'Flop' THEN 2
            WHEN 'Turn' THEN 3
            WHEN 'River' THEN 4
            ELSE 99 END AS StreetSort
FROM L2;
GO

/*** TEST RESULTS 

SELECT TOP 100 * FROM dbo.GameDetails

***/

--> 4 - Summarize Player results
SELECT Player,
        SUM(Amount) winnings,
        COUNT(Distinct GameId) games_played
FROM dbo.GameDetails
WHERE action <> 'rake' --exclude casino profits
GROUP BY Player
ORDER BY winnings DESC

--> 5 - CREATE EXTERNAL DATA SOURCE FOR LOCAL ADLS GEN2

--Create schemas for new objects
DECLARE @schemasql varchar(50)
IF ((SELECT COUNT(*) FROM sys.schemas where name = 'raw') = 0)
BEGIN
    SET @schemasql = N'CREATE SCHEMA raw'
    EXEC sp_executesql @tsql = @schemasql
END
GO

IF ((SELECT COUNT(*) FROM sys.external_data_sources WHERE name = 'PokerPRQData') > 0)
    DROP EXTERNAL DATA SOURCE PokerPRQData


DECLARE @datasrcsql AS NVARCHAR(300),
        @LakeAccount as NVARCHAR(200) = 'azrawdatalake4djynq', --Add name of local ADLS Gen2 storage account
        @RawFilePath AS NVARCHAR(200) = 'raw/Poker Data/PokerGameDetails' --Add local filepath for raw data lake zone storage

SET @datasrcsql = N'CREATE EXTERNAL DATA SOURCE PokerPRQData
        WITH 
        (  
            LOCATION = ''https://'+ @LakeAccount + '.dfs.core.windows.net/' + @RawFilePath + '''
        );' 
EXEC sp_executesql @tsql = @datasrcsql
GO

--Create the external file format
DECLARE  @fileformatsql AS NVARCHAR(300)
IF ((SELECT COUNT(*) FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') = 0)
BEGIN
    SET @fileformatsql = N'CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] WITH ( FORMAT_TYPE = PARQUET )'
    EXEC sp_executesql @tsql = @fileformatsql
END

-- use CETAS to export select statement with OPENROWSET result to  storage and partition by date
-- (NOTE: THIS QUERY WILL WRITE DATA TO /Poker GameDetails. If this directory already exists it will need to be deleted before execution)
CREATE EXTERNAL TABLE raw.gameDetails
WITH (
    LOCATION = '/',
    DATA_SOURCE = PokerPRQData,  
    FILE_FORMAT = SynapseParquetFormat
)  
AS
SELECT *
FROM dbo.GameDetails

--> 6 - Create View using OPENROWSET to query new Parquet data
DROP VIEW IF EXISTS raw.vwGameDetails
GO

CREATE VIEW raw.vwGameDetails
AS
SELECT *    
FROM
OPENROWSET
    (
        BULK '/*.parquet',     
        DATA_SOURCE = 'PokerPRQData',
        FORMAT = 'Parquet'
    ) AS [result]

/*** TEST results 

SELECT TOP 100 *
FROM raw.vwGameDetails

***/