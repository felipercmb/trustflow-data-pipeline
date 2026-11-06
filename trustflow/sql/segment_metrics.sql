SELECT channel, document_type,
    COUNT(*) AS requested_count,
    SUM(CASE WHEN terminal_type = 'signed' THEN 1 ELSE 0 END) AS signed_count,
    SUM(CASE WHEN terminal_type = 'failed' THEN 1 ELSE 0 END) AS failed_count,
    SUM(CASE WHEN terminal_type = 'canceled' THEN 1 ELSE 0 END) AS canceled_count,
    SUM(CASE WHEN terminal_type = 'expired' THEN 1 ELSE 0 END) AS expired_count,
    SUM(terminal_type IS NULL) AS pending_count,
    SUM(terminal_type IS NULL AND requested_at <= :sla_cutoff) AS overdue_pending_count,
    SUM(requested_at <= :sla_cutoff) AS sla_due_count,
    SUM(CASE WHEN requested_at <= :sla_cutoff AND terminal_type = 'signed'
        AND terminal_us - requested_us <= :sla_seconds * 1000000 THEN 1 ELSE 0 END) AS sla_met_count,
    ROUND(AVG(CASE WHEN terminal_type='signed' THEN (terminal_us-requested_us)/1000000.0 END), 2)
        AS avg_signed_seconds
FROM snapshot_workflows WHERE requested_at IS NOT NULL
GROUP BY channel, document_type
ORDER BY channel, document_type;
