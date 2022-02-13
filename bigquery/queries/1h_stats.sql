WITH 
    base_stats AS (
        SELECT
            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 15 FOLLOWING) avg_price_l15,
            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 30 FOLLOWING) avg_price_l30,
            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 60 FOLLOWING) avg_price_l60,
            MAX(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 15 FOLLOWING) max_price_l15,
            MAX(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 30 FOLLOWING) max_price_l30,
            MAX(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 60 FOLLOWING) max_price_l60,
            MAX(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 120 FOLLOWING) max_price_l120,
            MAX(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 240 FOLLOWING) max_price_l240,
            MAX(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 720 FOLLOWING) max_price_l720,
            MAX(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 1440 FOLLOWING) max_price_l1440,
            MIN(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 15 FOLLOWING) min_price_l15,
            MIN(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 30 FOLLOWING) min_price_l30,
            MIN(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 60 FOLLOWING) min_price_l60,
            MIN(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 120 FOLLOWING) min_price_l120,
            MIN(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 240 FOLLOWING) min_price_l240,
            MIN(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 720 FOLLOWING) min_price_l720,
            MIN(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 1440 FOLLOWING) min_price_l1440
        FROM tradeinsight.tech.predictions
        WHERE pair = "ETHUSDT"
        WINDOW prec_win AS (PARTITION BY pair ORDER BY Time DESC )
        ORDER BY Time DESC
        LIMIT 1
    )
SELECT * FROM base_stats
