-- Daily transaction summary
-- Aggregates transaction volumes and amounts by day and status.

CREATE OR REPLACE VIEW analytics.daily_summary AS
SELECT
    DATE(t.transaction_date)              AS transaction_day,
    t.status,
    COUNT(*)                              AS transaction_count,
    SUM(t.amount)                         AS total_amount,
    ROUND(AVG(t.amount), 2)              AS avg_amount,
    COUNT(DISTINCT t.merchant_id)         AS active_merchants,
    COUNT(DISTINCT t.customer_id)         AS unique_customers
FROM transactions t
WHERE t.status IN ('completed', 'pending', 'refunded')
GROUP BY DATE(t.transaction_date), t.status
ORDER BY transaction_day DESC, t.status;
