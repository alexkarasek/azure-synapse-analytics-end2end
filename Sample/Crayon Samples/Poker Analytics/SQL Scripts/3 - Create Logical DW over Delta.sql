/***** USE THE SCRIPTS BELOW TO QUERY Create Logical DW based on Delta Lake *****

1 - CREATE SQL VIEWS OVER Lake Data
2 - USE CETAS TO PERSIST SUMMARY DATA
3 - USE SQL VIEWS TO CREATE LOGICAL DW

[Convert GameDetails to Delta] Notebook can be executed once queries below have been run successfully

*****/

USE Pokerdata;
GO

CREATE SCHEMA ext;
GO

--Provide the data lake account and full file path of the enriched Delta game details
DECLARE @LakeAccount AS NVARCHAR(1000) = N'[insert storage account here]'
DECLARE @RawFilePath AS NVARCHAR(1000) = N'curated/Poker Data/PokerGameDetails'
DECLARE @Location    AS NVARCHAR(1000) = N''
DECLARE @SQLDataSrc  AS NVARCHAR(1000) = N''

--Create external data source PokerDeltaDataSrc
SET @Location = 'https://' + @LakeAccount + '.dfs.core.windows.net/' + @RawFilePath
SET @SQLDataSrc = N'CREATE EXTERNAL DATA SOURCE PokerDeltaDataSrc
            WITH 
            (  
                LOCATION = '''+ @Location + '''
            );' 

EXEC sp_executesql @tsql = @SQLDataSrc
GO

--Create view ext.GameDetails (view is based off the Delta poker game details in the enriched containter)
CREATE VIEW ext.GameDetails (GameId, TableName, Board, GameTime, GameActionId, Player, StreetName, Action, Amount, Cards, StreetSort, [datepart])--, loadid)
AS 
SELECT GameId
    , TableName
    , Board
    , GameTime
    , GameActionId
    , Player
    , StreetName
    , [Action]
    , Amount
    , Cards
    , StreetSort 
    , CAST(GameTime AS DATE) AS [datepart]
    --, loadid
FROM
    OPENROWSET(
        BULK '/',
        DATA_SOURCE = 'PokerDeltaDataSrc',
        FORMAT = 'DELTA'
    ) AS [result];
GO    

/*
    SELECT TOP 100 *
    FROM ext.GameDetails

*/


--Create view ext.Games
CREATE VIEW ext.Games (GameId, TableName, GameDate, Board, flop1, flop2, flop3, turn, river)
AS
SELECT DISTINCT GameId
    , TableName
    , CAST(GameTime AS DATE) AS GameDate
    , Board
    , SUBSTRING(REPLACE(REPLACE(REPLACE(Board,'[',''),']',''),' ',''), 1,2) AS flop1
    , SUBSTRING(REPLACE(REPLACE(REPLACE(Board,'[',''),']',''),' ',''), 3,2) AS flop2
    , SUBSTRING(REPLACE(REPLACE(REPLACE(Board,'[',''),']',''),' ',''), 5,2) AS flop3
    , SUBSTRING(REPLACE(REPLACE(REPLACE(Board,'[',''),']',''),' ',''), 7,2) AS turn
    , SUBSTRING(REPLACE(REPLACE(REPLACE(Board,'[',''),']',''),' ',''), 9,2) AS river
FROM ext.GameDetails
GO

/*
    SELECT TOP 100 *
    FROM ext.Games

*/

--Create schema CETAS
CREATE SCHEMA cetas;
GO

--Create the view cetas.PlayerGameSummary
--This view pre-calculates the key measures vpipCt, pfrCt, GameCt, Winnings by Game, Player, and GameDate.
--This is the view reference in CREATE EXTERNAL TABLE AS SELECT (CETAS) command.
CREATE VIEW cetas.vwPlayerGameSummary (GameId, Player, GameDate, vpipCt, pfrCt, GameCt, Winnings, Cards) 
AS 
WITH T1 AS (
SELECT player, GameId, gameTime
	, CAST(gameTime AS DATE) AS GameDate
	, MAX(
			CASE WHEN action = 'bet' THEN 1
				WHEN action = 'raise' THEN 1
				WHEN action = 'call' THEN 1
				ELSE 0
				END 
		 ) AS vpipct
	, MAX(
			CASE WHEN action = 'raise' THEN 1 
				ELSE 0
				END
		) AS pfrCt
	, 1 AS GameCt
FROM ext.GameDetails
WHERE streetName = 'Pre-flop'
GROUP BY player, GameId, gameTime
)
, T2 AS (
SELECT player, GameId, gameTime
	, SUM(amount) AS Profit
    , MAX(cards) AS Cards
FROM ext.GameDetails
GROUP BY player, GameId, gameTime
)
SELECT CAST(T1.GameId AS VARCHAR(100)) AS GameId
	, CAST(T1.Player AS VARCHAR(100)) AS Player
	, T1.GameDate
	, SUM(T1.vpipct) AS vpipCt
	, SUM(T1.pfrct)  AS pfrCt
	, SUM(T1.GameCt) AS GameCt
	, SUM(T2.Profit) AS Winnings
    , MAX(T2.Cards) AS Cards
FROM T1
INNER JOIN T2 ON T1.GameId = T2.GameId AND
	T1.Player = T2.Player
GROUP BY T1.GameId, T1.Player, T1.GameDate;
GO

/*
    SELECT TOP 100 *
    FROM cetas.vwPlayerGameSummary

*/

--Provide the data lake account and full file path where the pre-calculated dataset will reside in the data lake
DECLARE @lakeaccount AS NVARCHAR(100) = N'[insert storage account here]'
DECLARE @filepath    AS NVARCHAR(100) = N'curated/Poker Data/PlayerGameSummary' 

DECLARE @location    AS NVARCHAR(100) = N''
DECLARE @datasrcsql  AS NVARCHAR(300) = N''

--Create external data source PlayerGameSummary
IF ((SELECT COUNT(*) FROM sys.external_data_sources WHERE name = 'PlayerGameSummary') = 0)
BEGIN
    SET @location = 'https://'+ @LakeAccount + '.dfs.core.windows.net/'+ @filepath
    SET @datasrcsql = N'CREATE EXTERNAL DATA SOURCE PlayerGameSummary
                            WITH 
                                (  
                                    LOCATION = '''+ @location  + '''
                                );' 
    EXEC sp_executesql @tsql = @datasrcsql
END
GO

--Create the external file format
IF (NOT EXISTS(SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat')) BEGIN
    CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] WITH ( FORMAT_TYPE = PARQUET );
END
GO

--Generate the dynamic SQL that uses CETAS (CREATE EXTERNAL TABLE AS SELECT) to create the external table and export the View data
CREATE EXTERNAL TABLE cetas.PlayerGameSummary
WITH (
    LOCATION = '/',
    DATA_SOURCE = PlayerGameSummary,  
    FILE_FORMAT = SynapseParquetFormat
)  
AS
SELECT *
FROM cetas.vwPlayerGameSummary;

--Create the view ext.PlayerGameSummary to show the current pre-aggregated data
CREATE VIEW ext.PlayerGameSummary
AS
SELECT *    
FROM
OPENROWSET
(
    BULK '/*.parquet',     
    DATA_SOURCE = 'PlayerGameSummary',
    FORMAT = 'Parquet'
) AS fct


/*
    SELECT TOP 100 *
    FROM ext.PlayerGameSummary

*/

--Create view ext.DailyGameStats
CREATE VIEW ext.DailyGameStats
AS
SELECT Player
        ,GameDate
        ,1 as PlayerCt
        ,GameCt
        ,Winnings
FROM ext.PlayerGameSummary;
GO

/*
    SELECT TOP 100 *
    FROM ext.DailyGameStats

*/

--Create view ext.PlayerSummary
CREATE VIEW ext.PlayerSummary
AS
SELECT [player]
,[VPIP_Ct]
,[PFR_Ct]
,[Game_Ct]
,[Winnings]
,[WinningsPer100]
,[PFR]
,[VP$IP]
,[pfr_rate]
,[playertype]
,[labels]
 FROM [lakepokerdata].[dbo].[playersummary];
 GO

/*
    SELECT TOP 100 *
    FROM ext.PlayerSummary

*/


