-- Create view with bucket and hour discretization
WITH bucketed(hour, bucket, cnt) AS (
	SELECT hour(e.timestamp) as hour,
    CASE 
		WHEN e.magnitude > 6 THEN 6
        WHEN e.magnitude < 0 OR e.magnitude = NULL THEN 0
        ELSE floor(e.magnitude)
    END AS bucket, count(*)
    FROM earthquakes.event e
    group by bucket, hour
)
SELECT b.*
-- Get the number of rows of the most likely hour per bucket
FROM ( 
	SELECT bucket, max(cnt) as cnt
    FROM bucketed
    GROUP BY bucket
) cnt_max
-- Join with original to get other columns
INNER JOIN bucketed b on b.bucket = cnt_max.bucket and b.cnt = cnt_max.cnt 
ORDER BY b.bucket