-- What Redshift Serverless has charged, per day: RPU-seconds billed
-- (including the 60-second minimums) and their cost at the Sydney price of
-- $0.419 per RPU-hour (AWS Price List API, APS2-Redshift:ServerlessUsage).
SELECT
    trunc(start_time)                                       AS day,
    round(sum(charged_seconds) / 3600.0, 4)                 AS rpu_hours,
    round(sum(charged_seconds) / 3600.0 * 0.419, 4)         AS cost_usd
FROM sys_serverless_usage
GROUP BY 1
ORDER BY 1
