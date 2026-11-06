SELECT workflow_id, MIN(channel) AS channel, MIN(document_type) AS document_type,
    MAX(CASE WHEN event_type = 'requested' THEN occurred_at END) AS requested_at,
    MAX(CASE WHEN event_type <> 'requested' THEN occurred_at END) AS terminal_at,
    MAX(CASE WHEN event_type = 'requested' THEN occurred_at_us END) AS requested_us,
    MAX(CASE WHEN event_type <> 'requested' THEN occurred_at_us END) AS terminal_us,
    MAX(CASE WHEN event_type <> 'requested' THEN event_type END) AS terminal_type
FROM snapshot_events GROUP BY workflow_id
