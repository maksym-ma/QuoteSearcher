WITH 
    prediction AS (
        SELECT
            true label, 0.5 prob
        /*SELECT
            label, prob
        FROM
            ML.PREDICT(
                MODEL models.kline_features_5_9,
                (
                    SELECT
                        *
                    FROM
                        raw.short_klines_latest_ethusdt
                    LIMIT 1
                )
            ), UNNEST(predicted_label1_probs) pl
        WHERE label IS True*/
    ),
    buy_stats AS (
        SELECT 
            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 15 FOLLOWING) avg_buy_l15, 
            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 8 FOLLOWING) avg_buy_l8,
            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 3 FOLLOWING) avg_buy_l3
        FROM tradeinsight.tech.predictions        
        WHERE status = 'success' AND pair = "ETHUSDT" AND prediction = 1
        WINDOW prec_win AS (PARTITION BY pair, status ORDER BY Time DESC )
        ORDER BY Time DESC
        LIMIT 1
    ),
    sell_stats AS (
        SELECT 
            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 15 FOLLOWING) avg_sell_l15, 
            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 8 FOLLOWING) avg_sell_l8,
            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 3 FOLLOWING) avg_sell_l3
        FROM tradeinsight.tech.predictions        
        WHERE status = 'success' AND pair = "ETHUSDT" AND prediction = -1
        WINDOW prec_win AS (PARTITION BY pair, status ORDER BY Time DESC )
        ORDER BY Time DESC
        LIMIT 1
    ),
    base_stats AS (
        SELECT

            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 14 FOLLOWING) avg_price_l15,

            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 3 FOLLOWING) avg_price_l4,
            AVG(price) OVER (prec_win ROWS BETWEEN 1 FOLLOWING AND 4 FOLLOWING) pre1_avg_price_l4,
            AVG(price) OVER (prec_win ROWS BETWEEN 2 FOLLOWING AND 5 FOLLOWING) pre2_avg_price_l4,
            AVG(price) OVER (prec_win ROWS BETWEEN 3 FOLLOWING AND 6 FOLLOWING) pre3_avg_price_l4,

            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 6 FOLLOWING) avg_price_l7,
            AVG(price) OVER (prec_win ROWS BETWEEN 1 FOLLOWING AND 7 FOLLOWING) pre1_avg_price_l7,
            AVG(price) OVER (prec_win ROWS BETWEEN 2 FOLLOWING AND 8 FOLLOWING) pre2_avg_price_l7,
            AVG(price) OVER (prec_win ROWS BETWEEN 3 FOLLOWING AND 9 FOLLOWING) pre3_avg_price_l7,
            AVG(price) OVER (prec_win ROWS BETWEEN 4 FOLLOWING AND 10 FOLLOWING) pre4_avg_price_l7,
            AVG(price) OVER (prec_win ROWS BETWEEN 5 FOLLOWING AND 11 FOLLOWING) pre5_avg_price_l7,
            AVG(price) OVER (prec_win ROWS BETWEEN 6 FOLLOWING AND 12 FOLLOWING) pre6_avg_price_l7,
            AVG(price) OVER (prec_win ROWS BETWEEN 7 FOLLOWING AND 13 FOLLOWING) pre7_avg_price_l7,
            AVG(price) OVER (prec_win ROWS BETWEEN 8 FOLLOWING AND 14 FOLLOWING) pre8_avg_price_l7,
            AVG(price) OVER (prec_win ROWS BETWEEN 9 FOLLOWING AND 15 FOLLOWING) pre9_avg_price_l7,


            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 11 FOLLOWING) avg_price_l12,
            AVG(price) OVER (prec_win ROWS BETWEEN 1 FOLLOWING AND 12 FOLLOWING) pre1_avg_price_l12,
            AVG(price) OVER (prec_win ROWS BETWEEN 2 FOLLOWING AND 13 FOLLOWING) pre2_avg_price_l12,
            AVG(price) OVER (prec_win ROWS BETWEEN 3 FOLLOWING AND 14 FOLLOWING) pre3_avg_price_l12,

            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 24 FOLLOWING) avg_price_l25,
            AVG(price) OVER (prec_win ROWS BETWEEN 1 FOLLOWING AND 25 FOLLOWING) pre1_avg_price_l25,
            AVG(price) OVER (prec_win ROWS BETWEEN 2 FOLLOWING AND 26 FOLLOWING) pre2_avg_price_l25,
            AVG(price) OVER (prec_win ROWS BETWEEN 3 FOLLOWING AND 27 FOLLOWING) pre3_avg_price_l25,
            AVG(price) OVER (prec_win ROWS BETWEEN 10 FOLLOWING AND 34 FOLLOWING) pre10_avg_price_l25,

            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 49 FOLLOWING) avg_price_l50,
            AVG(price) OVER (prec_win ROWS BETWEEN 1 FOLLOWING AND 50 FOLLOWING) pre1_avg_price_l50,
            AVG(price) OVER (prec_win ROWS BETWEEN 2 FOLLOWING AND 51 FOLLOWING) pre2_avg_price_l50,
            AVG(price) OVER (prec_win ROWS BETWEEN 3 FOLLOWING AND 52 FOLLOWING) pre3_avg_price_l50,

            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 719 FOLLOWING) avg_price_l720,
            AVG(price) OVER (prec_win ROWS BETWEEN 30 FOLLOWING AND 749 FOLLOWING) pre30_avg_price_l720,

            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 29 FOLLOWING) avg_price_l30,
            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 59 FOLLOWING) avg_price_l60,
            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 89 FOLLOWING) avg_price_l90,
            AVG(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 119 FOLLOWING) avg_price_l120,
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
            MIN(price) OVER (prec_win ROWS BETWEEN 0 FOLLOWING AND 1440 FOLLOWING) min_price_l1440,
            preb last_pred_l0,
            LEAD(preb, 1) OVER (ORDER BY Time DESC ) last_pred_l1,
            LEAD(preb, 2) OVER (ORDER BY Time DESC ) last_pred_l2,
            LEAD(preb, 3) OVER (ORDER BY Time DESC ) last_pred_l3,
            ARRAY_AGG(price) OVER(prec_win ROWS BETWEEN 0 FOLLOWING AND 60 FOLLOWING) prices_list,
            LEAD(price, 1440) OVER(ORDER BY Time DESC ) price24h,
            LEAD(price, 720) OVER(ORDER BY Time DESC ) price12h
        FROM tradeinsight.tech.predictions        
        WHERE pair = "ETHUSDT"
        WINDOW prec_win AS (PARTITION BY pair ORDER BY Time DESC )
        ORDER BY Time DESC
        LIMIT 1
    )
SELECT
    *,
    (avg_price_l12 + avg_price_l7+pre1_avg_price_l7+pre2_avg_price_l7+pre3_avg_price_l7+pre4_avg_price_l7+pre5_avg_price_l7+pre6_avg_price_l7)/8 smoothAvg7,
    (avg_price_l12 + pre1_avg_price_l7+pre2_avg_price_l7+pre3_avg_price_l7+pre4_avg_price_l7+pre5_avg_price_l7+pre6_avg_price_l7+pre7_avg_price_l7)/8 pre1smoothAvg7
FROM prediction
CROSS JOIN base_stats
CROSS JOIN buy_stats 
CROSS JOIN sell_stats 
 