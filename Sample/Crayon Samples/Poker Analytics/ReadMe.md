<p align="center">
 <img src="./images/PokerTitle.png"
 </p>
 
## The Data Intelligence Solution Accelerator is intended to demonstrate a comprehensive platform for Advanced Analytics and AI built on top of Azure Synapse Analytics. In the Poker Analytics scenario, sample code is provided that can be used to query detailed historical logs for over 1 million hands of online poker stored as JSON in a public Azure storage container.<p>
 
By leveraging the capabilities of Azure Synapse Analytics, this data can be ingested, transformed, and analyzed in a way that makes it possible to analyze player behavior and uncover advanced insights around what drives profitability in this game of "chance".

Topics included in this accelerator include:
  * Azure Synapse SQL Serverless Pools
  * Azure Synapse Spark Pools
  * Logical DW/Delta Lake on ADLS Gen2
  * Azure ML
  * Data visualization and analytical reporting using Power BI
 
 ### Background:
 In poker, certain characteristics can be used to identify a player's style of play, and based on that style, specific strategies can be employed to maximize profits. As an example, below are 4 basic categories of players, along with an explanation of each, and observations about the optimal strategy to emply vs each.
 
 [Insert player quadrant slide]
 
As hands are played on online poker servers, logs are generated to capture all of the actions taking place. Examples of these types of logs have been generated and persisted as JSON for this solution. In order to derive insights, this accelerator can be used to parse the JSON data and persist it in a data lake where it can be transformed and enriched before ultimately being surfaced in Power BI.
 
 [Insert source data image]
 
 [Insert reference architecture image]
 
 See below for a demo of the dashboard that can be built using the code provided.
 
[Insert screenshot of demo with link to live page]
 
 In order to build this solution:
 
 * Follow instructions to deploy synapse workspace, storage account, and spark cluster
 * Import and execute script 0 to prep SQL Serverless database
 * Import and execute script 1 to query JSON data and create SQL Views
 * Import and execute notebook 2 to enrich data and persist in Delta Lake
 * Import and execute script 3 to create Logical DW over Delta Lake
