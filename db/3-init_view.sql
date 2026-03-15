CREATE MATERIALIZED VIEW mv_topic_growth AS
WITH date_series AS (
	SELECT generate_series(
        (SELECT MIN(DATE((timestamp AT TIME ZONE 'UTC'))) FROM logs WHERE action IN (5, 7)),
        (SELECT MAX(DATE((timestamp AT TIME ZONE 'UTC'))) FROM logs WHERE action IN (5, 7)),
        '1 day'::interval
    )::date AS day
),
daily_changes AS (
    SELECT 
        (timestamp AT TIME ZONE 'UTC')::date as day,
        COUNT(*) FILTER (WHERE action = 5 AND success = true) -
        COUNT(*) FILTER (WHERE action = 7 AND success = true) AS change
    FROM logs
    WHERE action IN (5, 7)
    GROUP BY DATE((timestamp AT TIME ZONE 'UTC'))
),
total_per_day AS (
	SELECT 
    	ds.day,
    	COALESCE(dc.change, 0) AS change,
    	SUM(COALESCE(dc.change, 0)) OVER (ORDER BY ds.day) AS total_topics
	FROM date_series AS ds
	LEFT JOIN daily_changes AS dc ON ds.day = dc.DAY
)
SELECT
	day,
	ROUND(change / NULLIF(LAG (total_topics) OVER (ORDER BY day), 0) * 100.0, 2) AS topic_growth_pct
FROM total_per_day;

CREATE UNIQUE INDEX ON mv_topic_growth (day);