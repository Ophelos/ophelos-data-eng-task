-- Weekly merchant performance report
-- Shows gross amount, refunds, and net amount per merchant per week.

CREATE OR REPLACE VIEW analytics.weekly_merchant_report AS
SELECT
    DATE_TRUNC('week', t.transaction_date)::DATE   AS week,
    m.merchant_id,
    m.merchant_name,
    m.category,
    COUNT(t.transaction_id)                         AS transaction_count,
    SUM(t.amount)                                   AS gross_amount,
    COALESCE(SUM(r.amount), 0)                      AS total_refunds,
    SUM(t.amount) - COALESCE(SUM(r.amount), 0)      AS net_amount,
    COUNT(DISTINCT t.customer_id)                    AS unique_customers
FROM transactions t
JOIN merchants m
    ON m.merchant_id = t.merchant_id
LEFT JOIN refunds r
    ON r.transaction_id = t.transaction_id
WHERE t.status IN ('completed', 'refunded')
GROUP BY
    DATE_TRUNC('week', t.transaction_date)::DATE,
    m.merchant_id,
    m.merchant_name,
    m.category
ORDER BY week DESC, gross_amount DESC;
