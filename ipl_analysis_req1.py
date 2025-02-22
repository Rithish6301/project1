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
WITH ball_stats AS (
    SELECT
        a.id AS ids,
        b.batter AS Batsman,
        b.batsman_run AS for_each,
        b.ballnumber AS balls,
        b.overs AS overs,
        ROW_NUMBER() OVER (PARTITION BY a.id, b.batter ORDER BY b.overs, b.ballnumber) AS no_of_balls
    FROM IPL_Matches a
    JOIN IPL_Ball_by_Ball b
    ON a.id = b.id
    WHERE YEAR(a.date) = 2008 AND b.extra_type <> 'wides'
),
cumulative_runs AS (
    SELECT
        no_of_balls,
        ids,
        Batsman,
        balls,
        overs,
        for_each,
        SUM(for_each) OVER (PARTITION BY ids, Batsman ORDER BY no_of_balls ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_runs
    FROM ball_stats
),
cumulative_runs_ranked AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY ids, Batsman ORDER BY cumulative_runs) AS no,
        *
    FROM cumulative_runs
    WHERE cumulative_runs BETWEEN 100 AND 105
),
batter_performance AS (
    SELECT
        b.id AS ids1,
        b.batter AS player,
        SUM(b.batsman_run) AS total_runs,
        SUM(CASE WHEN b.batsman_run = 4 THEN 1 ELSE 0 END) AS `4s`,
        SUM(CASE WHEN b.batsman_run = 6 THEN 1 ELSE 0 END) AS `6s`,
        CASE WHEN a.team1 <> b.BattingTeam THEN a.team1 ELSE a.team2 END AS opp,
        a.Venue AS Venue,
        a.date AS Date
    FROM IPL_Ball_by_Ball b
    JOIN IPL_Matches a ON a.id = b.id
    GROUP BY b.id, b.batter, a.team1, a.team2, b.BattingTeam, a.Venue, a.Date
    HAVING SUM(b.batsman_run) > 100
)
SELECT
    ROW_NUMBER() OVER (ORDER BY t.no_of_balls) AS POS,
    s.player AS player,
    s.total_runs AS Runs,
    t.no_of_balls AS BF,
    s.`4s` AS `4s`,
    s.`6s` AS `6s`,
    CASE
        WHEN s.opp = 'Mumbai Indians' THEN 'MI'
        WHEN s.opp = 'Chennai Super Kings' THEN 'CSK'
        WHEN s.opp = 'Royal Challengers Bangalore' THEN 'RCB'
        WHEN s.opp = 'Kolkata Knight Riders' THEN 'KKR'
        WHEN s.opp = 'Sunrisers Hyderabad' THEN 'SRH'
        WHEN s.opp = 'Deccan Chargers' THEN 'DC'
        WHEN s.opp = 'Punjab Kings' THEN 'PBKS'
        WHEN s.opp = 'Rajasthan Royals' THEN 'RR'
        WHEN s.opp = 'Gujarat Titans' THEN 'GT'
        WHEN s.opp = 'Lucknow Super Giants' THEN 'LSG'
        WHEN s.opp = 'Kings XI Punjab' THEN 'KXIP'
        WHEN s.opp = 'Delhi Daredevils' THEN 'DD'
        WHEN s.opp = 'Kochi Tuskers Kerala' THEN 'KTK'
        WHEN s.opp = 'Gujarat Lions' THEN 'GL'
        WHEN s.opp = 'Pune Warriors' THEN 'PW'
        WHEN s.opp = 'Rising Pune Supergiants' THEN 'RPS'
        WHEN s.opp = 'Delhi Capitals' THEN 'DC'
        ELSE s.opp
    END AS Against,
    s.Venue AS Venue,
    DATE_FORMAT(s.Date, 'dd-MMM-yy') AS `Match Date`
FROM batter_performance s
JOIN cumulative_runs_ranked t ON s.ids1 = t.ids
WHERE t.no = 1
"""

# Execute the query
result = spark.sql(sql_query)

# Show the result
result.show()