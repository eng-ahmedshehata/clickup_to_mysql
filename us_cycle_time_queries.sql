-- US Cycle Time Analysis Queries

-- 1. Show tasks with US cycle time (custom_item_id = '1001')
SELECT 
    task_id,
    task_name,
    custom_item_id,
    us_cycle_time,
    ROUND(us_cycle_time / 60, 2) as cycle_time_hours,
    ROUND(us_cycle_time / 1440, 2) as cycle_time_days,
    current_status
FROM tasks 
WHERE custom_item_id = '1001' AND us_cycle_time > 0
ORDER BY us_cycle_time DESC;

-- 2. US Cycle Time Statistics
SELECT 
    COUNT(*) as total_tasks,
    COUNT(CASE WHEN us_cycle_time > 0 THEN 1 END) as tasks_with_cycle_time,
    AVG(us_cycle_time) as avg_cycle_time_minutes,
    ROUND(AVG(us_cycle_time) / 60, 2) as avg_cycle_time_hours,
    ROUND(AVG(us_cycle_time) / 1440, 2) as avg_cycle_time_days,
    MIN(us_cycle_time) as min_cycle_time_minutes,
    MAX(us_cycle_time) as max_cycle_time_minutes,
    ROUND(MAX(us_cycle_time) / 60, 2) as max_cycle_time_hours
FROM tasks 
WHERE custom_item_id = '1001';

-- 3. US Cycle Time by Status
SELECT 
    t.current_status,
    COUNT(*) as task_count,
    AVG(t.us_cycle_time) as avg_cycle_time_minutes,
    ROUND(AVG(t.us_cycle_time) / 60, 2) as avg_cycle_time_hours,
    SUM(t.us_cycle_time) as total_cycle_time_minutes,
    ROUND(SUM(t.us_cycle_time) / 60, 2) as total_cycle_time_hours
FROM tasks t
WHERE t.custom_item_id = '1001' AND t.us_cycle_time > 0
GROUP BY t.current_status
ORDER BY avg_cycle_time_minutes DESC;

-- 4. Detailed US Cycle Time Breakdown
SELECT 
    t.task_id,
    t.task_name,
    t.us_cycle_time as total_cycle_time_minutes,
    ROUND(t.us_cycle_time / 60, 2) as total_cycle_time_hours,
    ttis.status,
    ttis.total_time as status_time_minutes,
    ROUND(ttis.total_time / 60, 2) as status_time_hours,
    ttis.from_date,
    ttis.to_date
FROM tasks t
JOIN task_time_in_status ttis ON t.task_id = ttis.task_id
WHERE t.custom_item_id = '1001' 
    AND t.us_cycle_time > 0
    AND ttis.status IN (
        'IN PROGRESS',
        'WAITING FOR FEEDBACK',
        'CODE REVIEW', 
        'DONE (DEVELOPMENT)',
        'READY FOR TESTING',
        'TESTING IN PROGRESS',
        'BUG FIXING',
        'RETESTING',
        'DONE'
    )
ORDER BY t.us_cycle_time DESC, ttis.from_date;

-- 5. US Cycle Time Performance Analysis
SELECT 
    CASE 
        WHEN us_cycle_time <= 1440 THEN '1 Day or Less'
        WHEN us_cycle_time <= 2880 THEN '1-2 Days'
        WHEN us_cycle_time <= 7200 THEN '2-5 Days'
        WHEN us_cycle_time <= 14400 THEN '5-10 Days'
        ELSE 'More than 10 Days'
    END as cycle_time_category,
    COUNT(*) as task_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM tasks 
WHERE custom_item_id = '1001' AND us_cycle_time > 0
GROUP BY 
    CASE 
        WHEN us_cycle_time <= 1440 THEN '1 Day or Less'
        WHEN us_cycle_time <= 2880 THEN '1-2 Days'
        WHEN us_cycle_time <= 7200 THEN '2-5 Days'
        WHEN us_cycle_time <= 14400 THEN '5-10 Days'
        ELSE 'More than 10 Days'
    END
ORDER BY 
    CASE cycle_time_category
        WHEN '1 Day or Less' THEN 1
        WHEN '1-2 Days' THEN 2
        WHEN '2-5 Days' THEN 3
        WHEN '5-10 Days' THEN 4
        ELSE 5
    END;

-- 6. Tasks with Longest US Cycle Time
SELECT 
    task_id,
    task_name,
    us_cycle_time,
    ROUND(us_cycle_time / 60, 2) as hours,
    ROUND(us_cycle_time / 1440, 2) as days,
    current_status,
    date_created,
    date_updated
FROM tasks 
WHERE custom_item_id = '1001' AND us_cycle_time > 0
ORDER BY us_cycle_time DESC
LIMIT 20;

-- 7. US Cycle Time vs Total Time Spent Comparison
SELECT 
    task_id,
    task_name,
    us_cycle_time as cycle_time_minutes,
    ROUND(us_cycle_time / 60, 2) as cycle_time_hours,
    time_spent as total_time_minutes,
    ROUND(time_spent / 60, 2) as total_time_hours,
    ROUND((us_cycle_time / time_spent) * 100, 2) as cycle_time_percentage
FROM tasks 
WHERE custom_item_id = '1001' 
    AND us_cycle_time > 0 
    AND time_spent > 0
ORDER BY cycle_time_percentage DESC;

-- 8. Monthly US Cycle Time Trends
SELECT 
    YEAR(date_created) as year,
    MONTH(date_created) as month,
    COUNT(*) as task_count,
    AVG(us_cycle_time) as avg_cycle_time_minutes,
    ROUND(AVG(us_cycle_time) / 60, 2) as avg_cycle_time_hours
FROM tasks 
WHERE custom_item_id = '1001' 
    AND us_cycle_time > 0
    AND date_created >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
GROUP BY YEAR(date_created), MONTH(date_created)
ORDER BY year DESC, month DESC; 