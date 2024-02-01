SELECT
    COUNT(*) FILTER(WHERE type in ('PRIVATE_HIRE','HOURLY_RENTAL')) AS approved_phv,
    COUNT(*) FILTER(WHERE type = 'TAXI') AS approved_taxi
FROM tada_member_service.driver
WHERE region = '{region}'
    AND approved = True
    AND date_trunc('month', DATE(approved_at AT TIME ZONE '{timezone}')) <= date_trunc('month', date '{date}')
