-- Caller supplies affected_workflows, avoiding one SELECT per workflow.
WITH grouped AS (
    SELECT e.workflow_id,
        SUM(event_type = 'requested') AS requests,
        SUM(event_type <> 'requested') AS terminals,
        COUNT(DISTINCT channel) AS channels,
        COUNT(DISTINCT document_type) AS documents,
        MAX(CASE WHEN event_type = 'requested' THEN occurred_at_us END) AS requested_us,
        MAX(CASE WHEN event_type <> 'requested' THEN occurred_at_us END) AS terminal_us
    FROM events e JOIN affected_workflows a USING(workflow_id)
    GROUP BY e.workflow_id
)
SELECT workflow_id, CASE
    WHEN requests > 1 OR terminals > 1 THEN 'LIFECYCLE_CONFLICT'
    WHEN channels > 1 OR documents > 1 THEN 'DIMENSION_CONFLICT'
    WHEN terminal_us < requested_us THEN 'TEMPORAL_ORDER'
END AS error_code
FROM grouped
WHERE requests > 1 OR terminals > 1 OR channels > 1 OR documents > 1
    OR terminal_us < requested_us
ORDER BY workflow_id
LIMIT 1;
