<p align="center">
 <img src="./images/PokerTitle.png"
 </p>
 
## The Modern Data Warehouse Solution Accelerator is intended to demonstrate a comprehensive platform for Advanced Analytics and AI built on top of Azure Synapse Analytics. In the Poker Analytics scenario, sample code is provided that can be used to query detailed historical logs for over 1 million hands of online poker stored as JSON in a public Azure storage container.<p>
 
By leveraging the capabilities of Azure Synapse Analytics, this data can be ingested, transformed, and analyzed in a way that makes it possible to analyze player behavior and uncover advanced insights around what drives profitability in this game of "chance".

Topics included in this accelerator include:
  * Azure Synapse SQL Serverless Pools
  * Azure Synapse Spark Pools
  * Logical DW/Delta Lake on ADLS Gen2
  * Azure ML
  * Data visualization and analytical reporting using Power BI
 
 ### Background:
 In poker, certain characteristics can be used to identify a player's style of play, and based on that style, specific strategies can be employed to maximize profits. As an example, below are 4 basic categories of players, along with an explanation of each, and observations about the optimal strategy to employ vs each.
 
<p align="center">
 <img src="./images/PokerMatrix.png">
 </p>
 
As hands are played on online poker servers, logs are generated to capture all of the actions taking place. Examples of these types of logs have been generated and persisted as JSON for this solution. In order to derive insights, this accelerator can be used to parse the JSON data and persist it in a data lake where it can be transformed and enriched before ultimately being surfaced in Power BI.
 
### Example of source data:<p>
 
 <p align="left">
  <img src="./images/PokerData.png">
  </p>
 
### In order to build this solution, we will implement architecture below:<p>
 
 <p align="left">
  <img src="./images/PokerArchitecture2.png">
            </p>
 
 ### Once the data is loaded and transformed, it can easily be served up for interactive analysis
 #### (Click on image to try it out!!)
 
<p align="center">
 <a href="https://app.powerbi.com/view?r=eyJrIjoiOTA3YmYwOTYtMDJiNS00MWM5LWEyZTktMGUzNDMyNzEyNzI4IiwidCI6IjY2ODk4MDgyLTM5NzktNGY3Mi1iYjY3LTU4YzNiODY0Zjk0YyJ9&pageName=ReportSectiona691326031230d3a0b69"> <img src="./images/PokerDemo.png"></img></a>
 </p>
 
 In order to build this solution:
 
 * Follow [instructions to deploy synapse workspace](/README.md), storage account, and spark cluster
 * Import and execute <a href="./SQL Scripts/0 - Setup Pokerdata Database.sql">script 0</a> to prep SQL Serverless database
 * Import and execute <a href="./SQL Scripts/./SQL Scripts/1 - Explore public JSON data.sql">script 1</a> to query the JSON data using Synapse SQL Serverless and OPENROWSET capabilities
 * Import and execute <a href="./Notebooks/2 - Convert Poker Game Data to Delta.ipynb">notebook 2</a> to enrich data and persist in Delta Lake
 * Import and execute <a href="./SQL Scripts/3 - Create Logical DW over Delta.sql">script 3</a> to create Logical DW over Delta Lake
 * Open <a href="./Power BI/Poker Analytics.pbit">Power BI Template file </a> and update paramters with the name of your SQL Serverless endpoint to populate dashboard based on your new LDW
