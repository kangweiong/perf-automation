SELECT
    COUNT(*) FILTER(WHERE date_trunc('month', DATE(created_at AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}')) AS driver_sign_up,
    COUNT(*) FILTER(WHERE date_trunc('month', DATE(created_at AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}')
                    AND date_trunc('month', DATE(approved_at AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}')) AS driver_same_month_approved,
    COUNT(*) FILTER(WHERE date_trunc('month', DATE(approved_at AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}')) AS driver_approved
FROM tada_member_service.driver
WHERE region = '{region}'
