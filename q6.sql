SELECT e.*
FROM (
	SELECT max(e.magnitude) max_mag
    FROM earthquakes.event e
) emax
INNER JOIN earthquakes.event e ON e.magnitude = emax.max_mag
