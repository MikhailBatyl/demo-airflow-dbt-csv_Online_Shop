WITH base_reviews AS (
    SELECT
        r.review_id,
        r.product_id,
        r.rating::numeric as rating,
        r.review_date::date as review_date,
        DATE_TRUNC('month', r.review_date::date)::date as month
    FROM {{ ref('stg_reviews') }} as r
),
product_stats AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category as category_name,
        COUNT(br.review_id) as reviews_count,
        AVG(br.rating) as avg_rating,
        SUM(CASE WHEN br.rating >= 4 THEN 1 ELSE 0 END) as positive_reviews,
        SUM(CASE WHEN br.rating <= 3 THEN 1 ELSE 0 END) as negative_reviews,
        ROUND(SUM(CASE WHEN br.rating >= 4 THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(br.review_id),0), 2) as positive_share,
        ROUND(SUM(CASE WHEN br.rating <= 3 THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(br.review_id),0), 2) as negative_share,
        CASE WHEN SUM(CASE WHEN br.rating <= 3 THEN 1 ELSE 0 END) = 0 THEN NULL ELSE ROUND(SUM(CASE WHEN br.rating >= 4 THEN 1 ELSE 0 END)::numeric / SUM(CASE WHEN br.rating <= 3 THEN 1 ELSE 0 END), 2)
        END as pos_neg_ratio
    FROM {{ ref('stg_products') }} as p
    LEFT JOIN base_reviews as br ON p.product_id = br.product_id
    GROUP BY p.product_id, p.product_name, p.category
),
global_stats AS (
    SELECT
        AVG(rating) as global_avg_rating,
        1623 as m
    FROM base_reviews
),
weighted_ratings AS (
    SELECT
        ps.*,
        ROUND(((ps.reviews_count::float / (ps.reviews_count + gs.m)) * ps.avg_rating::numeric + (gs.m::float / (ps.reviews_count + gs.m)) * gs.global_avg_rating::numeric)::numeric, 2) as weighted_rating
    FROM product_stats as ps
    CROSS JOIN global_stats as gs
),
monthly_reviews AS (
    SELECT
        br.month,
        br.product_id,
        COUNT(br.review_id) as monthly_reviews_count,
        ROUND(AVG(br.rating), 2) as monthly_avg_rating
    FROM base_reviews as br
    GROUP BY br.month, br.product_id
),
monthly_with_window AS (
    SELECT
        mr.*,
        SUM(mr.monthly_reviews_count) OVER (PARTITION BY mr.product_id ORDER BY mr.month) as cumulative_reviews,
        mr.monthly_avg_rating - LAG(mr.monthly_avg_rating) OVER (PARTITION BY mr.product_id ORDER BY mr.month) as delta_avg_rating,
        RANK() OVER (PARTITION BY mr.month ORDER BY mr.monthly_reviews_count DESC) as product_rank_by_reviews
    FROM monthly_reviews as mr
)
SELECT
    wr.product_id,
    wr.product_name,
    wr.category_name,
    wr.reviews_count,
    ROUND(wr.avg_rating,2) as avg_rating,
    wr.weighted_rating,
    wr.positive_reviews,
    wr.negative_reviews,
    wr.positive_share,
    wr.negative_share,
    wr.pos_neg_ratio,
    mw.month,
    mw.monthly_reviews_count,
    mw.monthly_avg_rating,
    mw.cumulative_reviews,
    mw.delta_avg_rating,
    mw.product_rank_by_reviews
FROM weighted_ratings as wr
LEFT JOIN monthly_with_window as mw ON wr.product_id = mw.product_id
ORDER BY mw.month DESC NULLS LAST, mw.product_rank_by_reviews
