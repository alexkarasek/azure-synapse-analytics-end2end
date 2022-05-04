if db_name() = 'master'
    throw 50001, 'This script cannot be executed in master database. Create new database and run the script there.', 1;

--Check for the existence of the database Pokerdata in SQL Serverless pool and create it if not found
IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'Pokerdata')
BEGIN
    CREATE DATABASE Pokerdata;
END
GO

--Change database context to 'Pokerdata'
USE Pokerdata
GO

--Clean up any pre-existing objects
--Drop Views
DROP VIEW IF EXISTS dbo.RawData
GO

DROP VIEW IF EXISTS dbo.GameDetails
GO

DROP VIEW IF EXISTS raw.vwGameDetails
GO
--Drop all views if present
DROP VIEW IF EXISTS ext.GameDetails ;
GO

DROP VIEW IF EXISTS ext.Games;
GO

DROP VIEW IF EXISTS cetas.vwPlayerGameSummary;
GO

DROP VIEW IF EXISTS ext.DailyGameStats;
GO

DROP VIEW IF EXISTS ext.PlayerSummary;
GO

DROP VIEW IF EXISTS ext.PlayerGameSummary;
GO


--Drop External Tables
IF (EXISTS(SELECT * FROM sys.external_tables WHERE name = 'gameDetails')) BEGIN
    DROP EXTERNAL TABLE raw.gameDetails
END

--Drop file formats
IF (EXISTS(SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat')) BEGIN
    DROP EXTERNAL FILE FORMAT SynapseParquetFormat
END

--Drop schemas
DROP SCHEMA IF EXISTS raw;
GO

DROP SCHEMA IF EXISTS ext;
GO

DROP SCHEMA IF EXISTS cetas;
GO


--Drop external data sources
IF (EXISTS(SELECT * FROM sys.external_data_sources WHERE name = 'pokerdata')) BEGIN
    DROP EXTERNAL DATA SOURCE pokerdata
END

IF (EXISTS(SELECT * FROM sys.external_data_sources WHERE name = 'PokerPRQData')) BEGIN
    DROP EXTERNAL DATA SOURCE PokerPRQData
END

IF (EXISTS(SELECT * FROM sys.external_data_sources WHERE name = 'PokerDeltaDataSrc')) BEGIN
    DROP EXTERNAL DATA SOURCE PokerDeltaDataSrc
END

IF (EXISTS(SELECT * FROM sys.external_data_sources WHERE name = 'PlayerGameSummary')) BEGIN
    DROP EXTERNAL DATA SOURCE PlayerGameSummary
END