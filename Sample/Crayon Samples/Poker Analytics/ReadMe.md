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
 
 In order to derive these insights, this accelerator can be used to transform JSON data and persist it in a data lake where it can be transformed and enriched before ultimately being surfaced in Power BI.
 
 [Insert source data image]
 
 [Insert reference architecture image]
 
 See below for a demo of the dashboard that can be built using the code provided.
 
<iframe src="https://app.powerbi.com/view?r=eyJrIjoiOTA3YmYwOTYtMDJiNS00MWM5LWEyZTktMGUzNDMyNzEyNzI4IiwidCI6IjY2ODk4MDgyLTM5NzktNGY3Mi1iYjY3LTU4YzNiODY0Zjk0YyJ9&pageName=ReportSectiona691326031230d3a0b69" frameborder="0" allowfullscreen="true"> </iframe>
 
 In order to build this solution:
 
 * Follow instructions to deploy synapse workspace, storage account, and spark cluster
 * Import and execute script 0 to prep SQL Serverless database
 * Import and execute script 1 to query JSON data and create SQL Views
 * Import and execute notebook 2 to enrich data and persist in Delta Lake
 * Import and execute script 3 to create Logical DW over Delta Lake
