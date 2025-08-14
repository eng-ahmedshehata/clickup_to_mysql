-- Example queries for the task_time_in_status table

-- 1. Get all task time in status records
SELECT 
    t.task_id,
    t.task_name,
    t.status,
    t.total_time,
    t.from_date,
    t.to_date,
    CASE 
        WHEN t.to_date IS NULL THEN 'Current Status'
        ELSE 'Historical Status'
    END as status_type
FROM task_time_in_status t
ORDER BY t.task_name, t.from_date;

-- 2. Get current status for all tasks
SELECT 
    t.task_id,
    t.task_name,
    t.status,
    t.total_time,
    t.from_date
FROM task_time_in_status t
WHERE t.to_date IS NULL
ORDER BY t.total_time DESC;

-- 3. Get total time spent in each status across all tasks
SELECT 
    t.status,
    COUNT(DISTINCT t.task_id) as task_count,
    SUM(t.total_time) as total_time_minutes,
    ROUND(SUM(t.total_time) / 60, 2) as total_time_hours,
    ROUND(AVG(t.total_time), 2) as avg_time_minutes
FROM task_time_in_status t
GROUP BY t.status
ORDER BY total_time_minutes DESC;

-- 4. Get tasks with longest time in current status
SELECT 
    t.task_id,
    t.task_name,
    t.status,
    t.total_time,
    ROUND(t.total_time / 60, 2) as hours,
    t.from_date,
    DATEDIFF(NOW(), t.from_date) as days_in_status
FROM task_time_in_status t
WHERE t.to_date IS NULL
ORDER BY t.total_time DESC
LIMIT 20;

-- 5. Get status transition timeline for a specific task
SELECT 
    t.task_id,
    t.task_name,
    t.status,
    t.total_time,
    t.from_date,
    t.to_date,
    DATEDIFF(t.to_date, t.from_date) as days_in_status
FROM task_time_in_status t
WHERE t.task_id = 'YOUR_TASK_ID_HERE'
ORDER BY t.from_date;

-- 6. Get tasks that have been in the same status for more than X days
SELECT 
    t.task_id,
    t.task_name,
    t.status,
    t.total_time,
    t.from_date,
    DATEDIFF(NOW(), t.from_date) as days_in_status
FROM task_time_in_status t
WHERE t.to_date IS NULL 
    AND DATEDIFF(NOW(), t.from_date) > 7  -- Change 7 to your desired number of days
ORDER BY days_in_status DESC;

-- 7. Get status efficiency analysis
SELECT 
    t.status,
    COUNT(DISTINCT t.task_id) as total_tasks,
    SUM(t.total_time) as total_time_minutes,
    ROUND(SUM(t.total_time) / COUNT(DISTINCT t.task_id), 2) as avg_time_per_task,
    ROUND(SUM(t.total_time) / 60, 2) as total_hours
FROM task_time_in_status t
WHERE t.to_date IS NOT NULL  -- Only completed status transitions
GROUP BY t.status
ORDER BY avg_time_per_task DESC;

-- 8. Get tasks with most status transitions
SELECT 
    t.task_id,
    t.task_name,
    COUNT(*) as status_transitions,
    SUM(t.total_time) as total_time_minutes,
    ROUND(SUM(t.total_time) / 60, 2) as total_hours
FROM task_time_in_status t
GROUP BY t.task_id, t.task_name
ORDER BY status_transitions DESC
LIMIT 20;

-- 9. Get status bottleneck analysis
SELECT 
    t.status,
    COUNT(DISTINCT t.task_id) as tasks_in_status,
    SUM(t.total_time) as total_time_minutes,
    ROUND(SUM(t.total_time) / 60, 2) as total_hours,
    ROUND(SUM(t.total_time) / COUNT(DISTINCT t.task_id), 2) as avg_time_per_task
FROM task_time_in_status t
WHERE t.to_date IS NULL  -- Current status only
GROUP BY t.status
ORDER BY total_time_minutes DESC;

-- 10. Get weekly status time summary
SELECT 
    YEARWEEK(t.from_date) as week,
    t.status,
    COUNT(DISTINCT t.task_id) as tasks,
    SUM(t.total_time) as total_time_minutes,
    ROUND(SUM(t.total_time) / 60, 2) as total_hours
FROM task_time_in_status t
WHERE t.from_date >= DATE_SUB(NOW(), INTERVAL 12 WEEK)
GROUP BY YEARWEEK(t.from_date), t.status
ORDER BY week DESC, total_time_minutes DESC;

-- 11. Get tasks that moved through statuses quickly
SELECT 
    t.task_id,
    t.task_name,
    COUNT(*) as status_count,
    SUM(t.total_time) as total_time_minutes,
    ROUND(SUM(t.total_time) / 60, 2) as total_hours,
    ROUND(SUM(t.total_time) / COUNT(*), 2) as avg_time_per_status
FROM task_time_in_status t
WHERE t.to_date IS NOT NULL  -- Only completed transitions
GROUP BY t.task_id, t.task_name
HAVING status_count > 2  -- Tasks with more than 2 status transitions
ORDER BY avg_time_per_status ASC
LIMIT 20;

-- 12. Get summary statistics
SELECT 
    COUNT(*) as total_status_records,
    COUNT(DISTINCT task_id) as total_tasks,
    COUNT(DISTINCT status) as unique_statuses,
    SUM(total_time) as total_time_minutes,
    ROUND(SUM(total_time) / 60, 2) as total_hours,
    ROUND(AVG(total_time), 2) as avg_time_per_status
FROM task_time_in_status;