-- SQLite
SELECT
    ea.variant,
    SUM(CASE WHEN ue.event_type = 'click' THEN 1 ELSE 0 END) AS total_clicks,
    SUM(CASE WHEN ue.event_type = 'page_view' THEN 1 ELSE 0 END) AS total_page_views,
    CAST(SUM(CASE WHEN ue.event_type = 'click' THEN 1 ELSE 0 END) AS REAL) * 100.0 /
    SUM(CASE WHEN ue.event_type = 'page_view' THEN 1 ELSE 0 END) AS click_through_rate_percent
FROM
    experiment_assignments AS ea
JOIN
    user_events AS ue ON ea.user_id = ue.user_id
JOIN
    experiments AS e ON ea.experiment_id = e.experiment_id
WHERE
    e.experiment_name = 'Experiment 1: New Homepage Layout' -- Эксперимент
    AND ue.event_timestamp BETWEEN e.start_date AND e.end_date -- События только в период эксперимента
GROUP BY
    ea.variant;


SELECT
    ea.variant,
    COUNT(DISTINCT ea.user_id) AS total_users_in_variant,
    COUNT(DISTINCT CASE WHEN ue.event_type = 'add_to_cart' THEN ue.user_id ELSE NULL END) AS users_added_to_cart,
    CAST(COUNT(DISTINCT CASE WHEN ue.event_type = 'add_to_cart' THEN ue.user_id ELSE NULL END) AS REAL) * 100.0 /
    COUNT(DISTINCT ea.user_id) AS add_to_cart_rate_percent
FROM
    experiment_assignments AS ea
LEFT JOIN -- LEFT JOIN, чтобы учесть всех пользователей в варианте, даже если у них нет событий
    user_events AS ue ON ea.user_id = ue.user_id
JOIN
    experiments AS e ON ea.experiment_id = e.experiment_id
WHERE
    e.experiment_name = 'Experiment 2: Button Color Test' -- Эксперимент
    AND ue.event_timestamp BETWEEN e.start_date AND e.end_date
GROUP BY
    ea.variant;


SELECT
    ea.variant,
    COUNT(DISTINCT ea.user_id) AS total_users_in_variant,
    COUNT(DISTINCT CASE WHEN ue.event_type = 'purchase' THEN ue.user_id ELSE NULL END) AS users_who_purchased,
    CAST(COUNT(DISTINCT CASE WHEN ue.event_type = 'purchase' THEN ue.user_id ELSE NULL END) AS REAL) * 100.0 /
    COUNT(DISTINCT ea.user_id) AS purchase_rate_percent
FROM
    experiment_assignments AS ea
LEFT JOIN
    user_events AS ue ON ea.user_id = ue.user_id
JOIN
    experiments AS e ON ea.experiment_id = e.experiment_id
WHERE
    e.experiment_name = 'Experiment 3: Checkout Flow Optimization' -- Эксперимент
    AND ue.event_timestamp BETWEEN e.start_date AND e.end_date
GROUP BY
    ea.variant;


