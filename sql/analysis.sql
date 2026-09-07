-- NYC 311 SQL Analysis
-- Source: transformed PySpark output

-- Top Complaint Types Within Each Borough and Their Percentage Share
WITH complaint_count AS (
    SELECT
        N.Borough,
        N.`Problem (formerly Complaint Type)`,
        COUNT(*) AS num_complaints
    FROM nyc_311 N
    WHERE N.Borough <> 'Unspecified'
    GROUP BY N.Borough, N.`Problem (formerly Complaint Type)`
),

ranked_complaints AS (
    SELECT
        CC.Borough,
        CC.`Problem (formerly Complaint Type)`,
        CC.num_complaints,
        SUM(CC.num_complaints) OVER(
            PARTITION BY CC.Borough
        ) AS borough_total,
        RANK() OVER (
            PARTITION BY CC.Borough ORDER BY CC.num_complaints DESC
        ) AS complaint_rank
    FROM complaint_count CC
)

SELECT
    RC.Borough,
    RC.`Problem (formerly Complaint Type)`,
    RC.num_complaints,
    RC.complaint_rank,
    ROUND(
        (RC.num_complaints * 100.0) / RC.borough_total, 2
    ) AS complaint_share_percentage
FROM ranked_complaints RC
WHERE RC.complaint_rank <= 3
ORDER BY RC.Borough, RC.complaint_rank;

-- High-Volume and Slow-Resolution Complaint Types
WITH request_count AS (
    SELECT
        N.`Problem (formerly Complaint Type)`,
        COUNT(*) AS total_request_count,
        COUNT(
            CASE 
                WHEN N.resolution_time_hours IS NOT NULL THEN 1 END
        ) AS resolved_request_count,
        ROUND(
            AVG(N.resolution_time_hours), 2
        ) AS average_resolution_time_hours
    FROM nyc_311 N
    GROUP BY N.`Problem (formerly Complaint Type)`
)

SELECT
    RC.`Problem (formerly Complaint Type)`,
    RC.total_request_count,
    RC.resolved_request_count,
    RC.average_resolution_time_hours
FROM request_count RC
WHERE RC.total_request_count > (
    SELECT AVG(total_request_count)
    FROM request_count
)
ORDER BY RC.average_resolution_time_hours DESC;

-- Agency Workload vs Resolution Speed
WITH agency_count AS (
    SELECT
        N.Agency,
        COUNT(*) AS total_requests,
        COUNT(
            CASE
                WHEN N.resolution_time_hours IS NOT NULL THEN 1 END
        ) AS resolved_requests,
        ROUND(
            AVG(N.resolution_time_hours), 2
        ) AS avg_resolution_hours
    FROM nyc_311 N
    GROUP BY N.Agency
),

ranked_agencies AS (
    SELECT
        AC.Agency,
        AC.total_requests,
        AC.resolved_requests,
        AC.avg_resolution_hours,
        RANK() OVER (
            ORDER BY AC.total_requests DESC
        ) AS workload_rank,
        CASE
            WHEN AC.avg_resolution_hours IS NOT NULL THEN
                RANK() OVER (
                    ORDER BY AC.avg_resolution_hours ASC NULLS LAST
                )
        END AS speed_rank
    FROM agency_count AC
)

SELECT
    RA.Agency,
    RA.total_requests,
    RA.resolved_requests,
    RA.avg_resolution_hours,
    RA.workload_rank,
    RA.speed_rank
FROM ranked_agencies RA
ORDER BY RA.workload_rank;

-- Borough Resolution Coverage and Speed
WITH borough_resolution AS (
    SELECT
        N.Borough,
        COUNT(*) AS total_request_count,
        COUNT(
            CASE
                WHEN N.resolution_time_hours IS NOT NULL THEN 1 END
        ) AS resolved_request_count,
        ROUND(
            AVG(N.resolution_time_hours), 2
        ) AS avg_resolution_time_hours
    FROM nyc_311 N
    WHERE N.Borough <> 'Unspecified'
    GROUP BY N.Borough
)

SELECT
    BR.Borough,
    BR.total_request_count,
    BR.resolved_request_count,
    BR.avg_resolution_time_hours,
    ROUND(
        (BR.resolved_request_count * 100.0) / BR.total_request_count, 2
    ) AS resolution_coverage_percentage
FROM borough_resolution BR
ORDER BY BR.avg_resolution_time_hours DESC;

-- Peak Hours for 311 Activity
WITH hourly_count AS (
    SELECT 
        N.request_hour,
        COUNT(*) AS hourly_request_count
    FROM nyc_311 N
    GROUP BY N.request_hour
),

hourly_ranks AS (
    SELECT
        HC.request_hour,
        HC.hourly_request_count,
        SUM(HC.hourly_request_count) OVER() AS total_requests,
        DENSE_RANK() OVER(
            ORDER BY HC.hourly_request_count DESC
        ) AS request_hour_rank
    FROM hourly_count HC
)

SELECT
    CONCAT(
        LPAD(
            CAST(HR.request_hour AS STRING), 2, '0'), ':00'
    ) AS request_hour,
    HR.hourly_request_count,
    HR.request_hour_rank,
    ROUND(
        (HR.hourly_request_count * 100.0) / HR.total_requests, 2
    ) AS hour_share_percentage
FROM hourly_ranks HR
ORDER BY HR.request_hour;

-- Day of Week and Weekday/Weekend Pattern
WITH date_bounds AS (
    SELECT
        MIN(N.request_date) AS first_date,
        MAX(N.request_date) AS last_date
    FROM nyc_311 N
),

daily_requests AS (
    SELECT
        N.request_date,
        N.request_day_of_week,
        COUNT(*) AS daily_request_count,
        ROUND(
            AVG(N.resolution_time_hours), 2
        ) AS average_resolution_hours
    FROM nyc_311 N
    CROSS JOIN date_bounds DB
    WHERE N.request_date > DB.first_date
        AND N.request_date < DB.last_date
    GROUP BY N.request_date, N.request_day_of_week
),

request_volume AS (
    SELECT
        DR.request_day_of_week,
        CASE
            WHEN DR.request_day_of_week IN ('Saturday', 'Sunday') THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        ROUND(
            AVG(DR.daily_request_count), 2
        ) AS average_requests_per_day,
        ROUND(
            AVG(DR.average_resolution_hours), 2
        ) AS average_resolution_hours,
        RANK() OVER (
            ORDER BY AVG(DR.daily_request_count) DESC
        ) AS day_rank
    FROM daily_requests DR
    GROUP BY DR.request_day_of_week
)

SELECT
    RV.request_day_of_week,
    RV.day_type,
    RV.average_requests_per_day,
    RV.average_resolution_hours,
    RV.day_rank
FROM request_volume RV
ORDER BY
    CASE
        WHEN RV.request_day_of_week = 'Sunday' THEN 1
        WHEN RV.request_day_of_week = 'Monday' THEN 2
        WHEN RV.request_day_of_week = 'Tuesday' THEN 3
        WHEN RV.request_day_of_week = 'Wednesday' THEN 4
        WHEN RV.request_day_of_week = 'Thursday' THEN 5
        WHEN RV.request_day_of_week = 'Friday' THEN 6
        WHEN RV.request_day_of_week = 'Saturday' THEN 7
    END;

-- Daily Spike vs the Average Day
WITH request_count AS (
    SELECT
        N.request_date,
        COUNT(*) AS daily_request_count
    FROM nyc_311 N
    GROUP BY N.request_date
),

avg_requests AS (
    SELECT
        ROUND(
            AVG(RC.daily_request_count), 2
        ) AS avg_daily_requests
    FROM request_count RC
)

SELECT
    RC.request_date,
    RC.daily_request_count,
    AR.avg_daily_requests,
    ROUND(
        (RC.daily_request_count - AR.avg_daily_requests) / NULLIF(AR.avg_daily_requests, 0) * 100.0, 2
    ) AS percentage_difference,
    CASE
        WHEN RC.daily_request_count > AR.avg_daily_requests THEN 'Above Average'
        WHEN RC.daily_request_count < AR.avg_daily_requests THEN 'Below Average'
        ELSE 'Average'
    END AS comparison_status
FROM request_count RC
CROSS JOIN avg_requests AR
ORDER BY RC.request_date;

-- Peak Hour By Complaint Type
WITH top_complaints AS (
    SELECT
        N.`Problem (formerly Complaint Type)`,
        COUNT(*) AS total_requests,
        RANK() OVER (
            ORDER BY COUNT(*) DESC
        ) AS complaint_rank
    FROM nyc_311 N
    GROUP BY N.`Problem (formerly Complaint Type)`
),

complaint_hours AS (
    SELECT
        N.`Problem (formerly Complaint Type)`,
        N.request_hour,
        COUNT(*) AS hourly_request_count,
        DENSE_RANK() OVER (
            PARTITION BY N.`Problem (formerly Complaint Type)`
            ORDER BY COUNT(*) DESC
        ) AS hour_rank
    FROM nyc_311 N
    JOIN top_complaints TC
        ON N.`Problem (formerly Complaint Type)` = TC.`Problem (formerly Complaint Type)`
    WHERE TC.complaint_rank <= 10
    GROUP BY N.`Problem (formerly Complaint Type)`, N.request_hour
)

SELECT
    CH.`Problem (formerly Complaint Type)`,
    CONCAT(
        LPAD(
            CAST(CH.request_hour AS STRING), 2, '0'), ':00'
    ) AS request_hour,
    CH.hourly_request_count,
    CH.hour_rank
FROM complaint_hours CH
WHERE CH.hour_rank <= 3
ORDER BY CH.`Problem (formerly Complaint Type)`, CH.hour_rank;

-- Complaint Resolution Differences Across Boroughs
WITH top_complaints AS (
    SELECT
        N.`Problem (formerly Complaint Type)`,
        COUNT(*) AS total_requests,
        RANK() OVER (
            ORDER BY COUNT(*) DESC
        ) AS complaint_rank
    FROM nyc_311 N
    GROUP BY N.`Problem (formerly Complaint Type)`
),

complaint_borough AS (
    SELECT
        N.Borough,
        N.`Problem (formerly Complaint Type)`,
        COUNT(
            CASE
                WHEN N.resolution_time_hours IS NOT NULL THEN 1 END
        ) AS resolved_requests,
        ROUND(
            AVG(N.resolution_time_hours), 2
        ) AS avg_resolution_hours,
        DENSE_RANK() OVER (
            PARTITION BY N.`Problem (formerly Complaint Type)`
            ORDER BY AVG(N.resolution_time_hours) DESC
        ) AS resolution_rank
    FROM nyc_311 N
    JOIN top_complaints TC
        ON N.`Problem (formerly Complaint Type)` = TC.`Problem (formerly Complaint Type)`
    WHERE TC.complaint_rank <= 10
        AND N.Borough <> 'Unspecified'
    GROUP BY
        N.Borough,
        N.`Problem (formerly Complaint Type)`
    HAVING COUNT(
        CASE
            WHEN N.resolution_time_hours IS NOT NULL THEN 1 END
    ) >= 100
)

SELECT
    CB.`Problem (formerly Complaint Type)`,
    CB.Borough,
    CB.resolved_requests,
    CB.avg_resolution_hours,
    CB.resolution_rank
FROM complaint_borough CB
ORDER BY
    CB.`Problem (formerly Complaint Type)`,
    CB.resolution_rank;