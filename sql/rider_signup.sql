SELECT
    COUNT(*) AS rider_signup
FROM tada_member_service.rider
WHERE date_trunc('month', DATE(created_at AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}')
  AND region = '{region}'