-- Merchant performance analysis
-- Comprehensive merchant metrics with ranking and trend indicators.

CREATE OR REPLACE VIEW analytics.merchant_performance AS
WITH merchant_txn_stats AS (
    SELECT
        t.merchant_id,
        COUNT(*)                                            AS total_transactions,
        SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END)  AS completed_count,
        SUM(CASE WHEN t.status = 'failed' THEN 1 ELSE 0 END)     AS failed_count,
        SUM(CASE WHEN t.status = 'refunded' THEN 1 ELSE 0 END)   AS refunded_count,
        SUM(CASE WHEN t.status = 'completed' THEN t.amount ELSE 0 END) AS completed_amount,
        COUNT(DISTINCT t.customer_id)                       AS unique_customers,
        MIN(t.transaction_date)                             AS first_transaction,
        MAX(t.transaction_date)                             AS last_transaction
    FROM transactions t
    GROUP BY t.merchant_id
),
merchant_refund_stats AS (
    SELECT
        t.merchant_id,
        COUNT(DISTINCT r.refund_id)                         AS refund_count,
        COALESCE(SUM(r.amount), 0)                          AS refund_amount
    FROM transactions t
    LEFT JOIN refunds r ON r.transaction_id = t.transaction_id
    WHERE t.status IN ('completed', 'refunded')
    GROUP BY t.merchant_id
),
merchant_daily AS (
    SELECT
        t.merchant_id,
        DATE(t.transaction_date) AS txn_date,
        SUM(t.amount) AS daily_amount
    FROM transactions t
    WHERE t.status = 'completed'
    GROUP BY t.merchant_id, DATE(t.transaction_date)
),
merchant_daily_ranked AS (
    SELECT
        merchant_id,
        txn_date,
        daily_amount,
        ROW_NUMBER() OVER (PARTITION BY merchant_id ORDER BY txn_date DESC) AS day_rank,
        AVG(daily_amount) OVER (
            PARTITION BY merchant_id
            ORDER BY txn_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_avg
    FROM merchant_daily
),
merchant_latest_daily AS (
    SELECT
        merchant_id,
        daily_amount AS latest_daily_amount,
        rolling_7d_avg AS latest_rolling_avg
    FROM merchant_daily_ranked
    WHERE day_rank = 1
)
SELECT
    m.merchant_id,
    m.merchant_name,
    m.category,
    m.country,
    m.status                                                AS merchant_status,
    mts.total_transactions,
    mts.completed_count,
    mts.failed_count,
    mts.refunded_count,
    ROUND(
        mts.completed_count::NUMERIC / NULLIF(mts.total_transactions, 0) * 100,
        2
    )                                                       AS success_rate_pct,
    mts.completed_amount                                    AS gross_completed_amount,
    mrs.refund_amount                                       AS total_refund_amount,
    mts.completed_amount - mrs.refund_amount                AS net_amount,
    ROUND(
        mrs.refund_amount / NULLIF(mts.completed_amount, 0) * 100,
        2
    )                                                       AS refund_rate_pct,
    mts.unique_customers,
    ROUND(
        mts.completed_amount / NULLIF(mts.completed_count, 0),
        2
    )                                                       AS avg_transaction_value,
    mts.first_transaction,
    mts.last_transaction,
    mld.latest_daily_amount,
    ROUND(mld.latest_rolling_avg, 2)                        AS rolling_7d_avg_amount,
    RANK() OVER (ORDER BY mts.completed_amount DESC)        AS revenue_rank,
    RANK() OVER (ORDER BY mts.unique_customers DESC)        AS customer_rank
FROM merchants m
JOIN merchant_txn_stats mts ON mts.merchant_id = m.merchant_id
LEFT JOIN merchant_refund_stats mrs ON mrs.merchant_id = m.merchant_id
LEFT JOIN merchant_latest_daily mld ON mld.merchant_id = m.merchant_id
ORDER BY mts.completed_amount DESC;
