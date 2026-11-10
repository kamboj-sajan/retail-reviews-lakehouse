-- Everything currently in quarantine, by category, month and reason.
SELECT category, load_month, reason, count(*) AS quarantined_rows
FROM rrl_quarantine.reviews
GROUP BY category, load_month, reason
ORDER BY load_month, category, reason
