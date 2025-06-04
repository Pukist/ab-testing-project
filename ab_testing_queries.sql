-- =============================================
-- Анализ Experiment 1: Click-Through Rate (CTR)
-- =============================================
SELECT
    ea.variant,  -- Группа (control/treatment)
    SUM(CASE WHEN ue.event_type = 'click' THEN 1 ELSE 0 END) AS total_clicks,  -- Общее количество кликов
    SUM(CASE WHEN ue.event_type = 'page_view' THEN 1 ELSE 0 END) AS total_page_views,  -- Общее количество просмотров страниц
    -- Расчет CTR в процентах: (клики / просмотры) * 100
    CAST(SUM(CASE WHEN ue.event_type = 'click' THEN 1 ELSE 0 END) AS REAL) * 100.0 /
    SUM(CASE WHEN ue.event_type = 'page_view' THEN 1 ELSE 0 END) AS click_through_rate_percent
FROM
    experiment_assignments AS ea  -- Таблица распределения по группам
JOIN
    user_events AS ue ON ea.user_id = ue.user_id  -- Соединяем с событиями пользователей
JOIN
    experiments AS e ON ea.experiment_id = e.experiment_id  -- Соединяем с информацией об эксперименте
WHERE
    e.experiment_name = 'Experiment 1: New Homepage Layout'  -- Фильтр по названию эксперимента
    -- Учитываем только события, произошедшие в период проведения эксперимента
    AND ue.event_timestamp BETWEEN e.start_date AND e.end_date
GROUP BY
    ea.variant;  -- Группируем результаты по вариантам теста


-- ================================================
-- Анализ Experiment 2: Add-to-Cart Rate (ATC Rate)
-- ================================================
SELECT
    ea.variant,
    COUNT(DISTINCT ea.user_id) AS total_users_in_variant,  -- Общее количество пользователей в группе
    -- Количество уникальных пользователей, добавивших товар в корзину
    COUNT(DISTINCT CASE WHEN ue.event_type = 'add_to_cart' THEN ue.user_id ELSE NULL END) AS users_added_to_cart,
    -- Расчет процента пользователей, добавивших в корзину
    CAST(COUNT(DISTINCT CASE WHEN ue.event_type = 'add_to_cart' THEN ue.user_id ELSE NULL END) AS REAL) * 100.0 /
    COUNT(DISTINCT ea.user_id) AS add_to_cart_rate_percent
FROM
    experiment_assignments AS ea
-- Используем LEFT JOIN чтобы включить всех пользователей, даже без событий
LEFT JOIN
    user_events AS ue ON ea.user_id = ue.user_id
JOIN
    experiments AS e ON ea.experiment_id = e.experiment_id
WHERE
    e.experiment_name = 'Experiment 2: Button Color Test'  -- Тест цвета кнопки
    -- События только в период эксперимента
    AND (ue.event_timestamp BETWEEN e.start_date AND e.end_date OR ue.event_timestamp IS NULL)
GROUP BY
    ea.variant;


-- =============================================
-- Анализ Experiment 3: Purchase Rate (Конверсия)
-- =============================================
SELECT
    ea.variant,
    COUNT(DISTINCT ea.user_id) AS total_users_in_variant,  -- Всего пользователей в группе
    -- Уникальные пользователи с покупками
    COUNT(DISTINCT CASE WHEN ue.event_type = 'purchase' THEN ue.user_id ELSE NULL END) AS users_who_purchased,
    -- Расчет конверсии в покупку (%)
    CAST(COUNT(DISTINCT CASE WHEN ue.event_type = 'purchase' THEN ue.user_id ELSE NULL END) AS REAL) * 100.0 /
    COUNT(DISTINCT ea.user_id) AS purchase_rate_percent
FROM
    experiment_assignments AS ea
-- LEFT JOIN для учета пользователей без событий
LEFT JOIN
    user_events AS ue ON ea.user_id = ue.user_id
JOIN
    experiments AS e ON ea.experiment_id = e.experiment_id
WHERE
    e.experiment_name = 'Experiment 3: Checkout Flow Optimization'  -- Оптимизация оформления заказа
    -- События только в период эксперимента (или NULL для пользователей без событий)
    AND (ue.event_timestamp BETWEEN e.start_date AND e.end_date OR ue.event_timestamp IS NULL)
GROUP BY
    ea.variant;

