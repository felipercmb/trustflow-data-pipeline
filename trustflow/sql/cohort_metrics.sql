-- Daily cohorts by requested_at (UTC), observed at :as_of.
-- Filtering events BEFORE aggregation avoids future outcomes leaking into past reports.
WITH observed AS (
    SELECT * FROM events
    WHERE occurred_at <= :as_of AND (:channel IS NULL OR channel = :channel)
), lifecycle AS (
    SELECT workflow_id,
        MAX(CASE WHEN event_type = 'requested' THEN occurred_at END) AS requested_at,
        MAX(CASE WHEN event_type <> 'requested' THEN occurred_at END) AS terminal_at,
        MAX(CASE WHEN event_type = 'requested' THEN occurred_at_us END) AS requested_us,
        MAX(CASE WHEN event_type <> 'requested' THEN occurred_at_us END) AS terminal_us,
        MAX(CASE WHEN event_type <> 'requested' THEN event_type END) AS terminal_type
    FROM observed GROUP BY workflow_id
), durations AS (
    SELECT *, SUBSTR(requested_at, 1, 10) AS cohort_date,
        (terminal_us - requested_us) / 1000000.0 AS duration_seconds,
        requested_at <= :sla_cutoff AS is_due
    FROM lifecycle WHERE requested_at IS NOT NULL
), ranked_signed AS (
    SELECT cohort_date, duration_seconds,
        ROW_NUMBER() OVER (PARTITION BY cohort_date ORDER BY duration_seconds, workflow_id) AS rank,
        COUNT(*) OVER (PARTITION BY cohort_date) AS n
    FROM durations WHERE terminal_type = 'signed'
), percentiles AS (
    SELECT cohort_date, duration_seconds AS p95_signed_seconds
    FROM ranked_signed WHERE rank = (95 * n + 99) / 100
)
SELECT d.cohort_date,
    COUNT(*) AS requested_count,
    SUM(CASE WHEN terminal_type = 'signed' THEN 1 ELSE 0 END) AS signed_count,
    SUM(CASE WHEN terminal_type = 'failed' THEN 1 ELSE 0 END) AS failed_count,
    SUM(CASE WHEN terminal_type = 'canceled' THEN 1 ELSE 0 END) AS canceled_count,
    SUM(CASE WHEN terminal_type = 'expired' THEN 1 ELSE 0 END) AS expired_count,
    SUM(terminal_type IS NULL) AS pending_count,
    SUM(terminal_type IS NULL AND is_due) AS overdue_pending_count,
    SUM(is_due) AS sla_due_count,
    SUM(CASE WHEN is_due AND terminal_type = 'signed' AND duration_seconds <= :sla_seconds
        THEN 1 ELSE 0 END) AS sla_met_count,
    ROUND(100.0 * SUM(CASE WHEN is_due AND terminal_type = 'signed'
        AND duration_seconds <= :sla_seconds THEN 1 ELSE 0 END) / NULLIF(SUM(is_due), 0), 2)
        AS sla_met_pct,
    ROUND(AVG(CASE WHEN terminal_type = 'signed' THEN duration_seconds END), 2)
        AS avg_signed_seconds,
    p.p95_signed_seconds
FROM durations d
LEFT JOIN percentiles p ON p.cohort_date = d.cohort_date
GROUP BY d.cohort_date
ORDER BY d.cohort_date;
