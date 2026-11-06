WITH pending AS (
    SELECT (:as_of_us - requested_us) / 1000000.0 AS age_seconds,
        CASE
            WHEN :as_of_us - requested_us < :sla_seconds * 1000000 THEN 0
            WHEN :as_of_us - requested_us < :sla_seconds * 4000000 THEN 1
            WHEN :as_of_us - requested_us < :sla_seconds * 24000000 THEN 2
            ELSE 3
        END AS age_band
    FROM snapshot_workflows
    WHERE requested_at IS NOT NULL AND terminal_type IS NULL
)
SELECT CASE age_band WHEN 0 THEN 'within_sla' WHEN 1 THEN 'one_to_four_sla'
        WHEN 2 THEN 'four_to_twenty_four_sla' ELSE 'over_twenty_four_sla' END AS bucket,
    COUNT(*) AS pending_count, MAX(age_seconds) AS oldest_age_seconds
FROM pending GROUP BY age_band ORDER BY age_band;
