from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .appName("ipl_match_data") \
    .getOrCreate()

# Load the datasets
df1 = spark.read.csv(
    r"C:\\Users\\rithish.yeravelli\\Downloads\\001_ProjectBRD\\001_ProjectBRD\\Project1-dataset\\IPL_Player_Performance_Dataset\\IPL_Ball_by_Ball_2008_2022.csv",
    header=True, inferSchema=True
)

df2 = spark.read.csv(
    r"C:\\Users\\rithish.yeravelli\\Downloads\\001_ProjectBRD\\001_ProjectBRD\\Project1-dataset\\IPL_Player_Performance_Dataset\\IPL_Matches_2008_2022.csv",
    header=True, inferSchema=True
)

# Create temporary views for SQL queries
df1.createOrReplaceTempView("IPL_Ball_by_Ball")
df2.createOrReplaceTempView("IPL_Matches")

# SQL query
sql_query = """
SELECT
    ROW_NUMBER() OVER (PARTITION BY YEAR(Date) ORDER BY SUM(s_run) DESC) AS POS,
    batter AS Player,
    COUNT(DISTINCT ID) AS Mat,
    COUNT(batter) AS Inn,
    COUNT(batter) - SUM(NO) AS NO,
    SUM(s_run) AS Runs,
    MAX(s_run) AS HS,
    ROUND(SUM(s_run) / (COUNT(batter) - SUM(NO)), 2) AS Avg,
    SUM(bf) AS BF,
    ROUND(SUM(s_run) / SUM(bf) * 100, 2) AS SR,
    SUM(CASE WHEN s_run >= 100 THEN 1 ELSE 0 END) AS "100",
    SUM(CASE WHEN s_run BETWEEN 50 AND 99 THEN 1 ELSE 0 END) AS "50",
    SUM(fours) AS "4s",
    SUM(sixes) AS "6s"
FROM (
    SELECT
        b.ID,
        b.batter,
        SUM(batsman_run) AS s_run,
        SUM(CASE WHEN extra_type NOT IN ('penalty', 'wides') THEN 1 ELSE 0 END) AS bf,
        SUM(CASE WHEN batsman_run = 4 THEN 1 ELSE 0 END) AS fours,
        SUM(CASE WHEN batsman_run = 6 THEN 1 ELSE 0 END) AS sixes,
        SUM(CASE WHEN player_out = batter THEN 1 ELSE 0 END) AS NO,
        m.Date
    FROM IPL_Ball_by_Ball b
    JOIN IPL_Matches m ON b.ID = m.ID
    WHERE m.SEASON = '2007/08'
    GROUP BY b.ID, b.batter, m.Date
) AS stats
GROUP BY batter, YEAR(Date)
"""

# Execute the query
result = spark.sql(sql_query)

# Show the result
result.show()