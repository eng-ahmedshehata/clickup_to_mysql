-- Example queries for the subtasks table

-- 1. Get all subtasks with their parent task information
SELECT 
    s.task_id,
    s.task_name,
    s.subtask_id,
    s.subtask_name,
    s.time_estimate,
    s.time_spent,
    s.status,
    s.tags_names
FROM subtasks s
ORDER BY s.task_name, s.subtask_name;

-- 2. Get total time estimates and time spent by parent task
SELECT 
    s.task_id,
    s.task_name,
    COUNT(s.subtask_id) as subtask_count,
    SUM(s.time_estimate) as total_time_estimate,
    SUM(s.time_spent) as total_time_spent
FROM subtasks s
GROUP BY s.task_id, s.task_name
ORDER BY total_time_spent DESC;

-- 3. Get subtasks by status
SELECT 
    s.status,
    COUNT(*) as subtask_count,
    SUM(s.time_estimate) as total_estimated,
    SUM(s.time_spent) as total_spent
FROM subtasks s
GROUP BY s.status
ORDER BY subtask_count DESC;

-- 4. Get subtasks with specific tags
SELECT 
    s.task_name,
    s.subtask_name,
    s.tags_names,
    s.time_estimate,
    s.time_spent,
    s.status
FROM subtasks s
WHERE s.tags_names LIKE '%bug%' OR s.tags_names LIKE '%urgent%'
ORDER BY s.time_spent DESC;

-- 5. Get tasks with the most subtasks
SELECT 
    s.task_id,
    s.task_name,
    COUNT(s.subtask_id) as subtask_count,
    SUM(s.time_estimate) as total_estimated,
    SUM(s.time_spent) as total_spent
FROM subtasks s
GROUP BY s.task_id, s.task_name
HAVING subtask_count > 5
ORDER BY subtask_count DESC;

-- 6. Get subtasks that are over/under estimated time
SELECT 
    s.task_name,
    s.subtask_name,
    s.time_estimate,
    s.time_spent,
    (s.time_spent - s.time_estimate) as time_difference,
    CASE 
        WHEN s.time_spent > s.time_estimate THEN 'Over Estimated'
        WHEN s.time_spent < s.time_estimate THEN 'Under Estimated'
        ELSE 'On Track'
    END as estimation_status
FROM subtasks s
WHERE s.time_estimate IS NOT NULL AND s.time_spent IS NOT NULL
ORDER BY ABS(s.time_spent - s.time_estimate) DESC;

-- 7. Get subtasks with no time tracking
SELECT 
    s.task_name,
    s.subtask_name,
    s.status,
    s.tags_names
FROM subtasks s
WHERE s.time_estimate IS NULL AND s.time_spent IS NULL;

-- 8. Get summary statistics
SELECT 
    COUNT(*) as total_subtasks,
    COUNT(DISTINCT task_id) as total_parent_tasks,
    AVG(time_estimate) as avg_time_estimate,
    AVG(time_spent) as avg_time_spent,
    SUM(time_estimate) as total_estimated_time,
    SUM(time_spent) as total_spent_time
FROM subtasks;

-- 9. Compare main task time vs subtasks time
SELECT 
    t.task_id,
    t.name as task_name,
    t.time_estimate as main_task_estimate,
    t.time_spent as main_task_spent,
    t.subtasks_time_estimate,
    t.subtasks_time_spent,
    (t.time_estimate + t.subtasks_time_estimate) as total_estimate,
    (t.time_spent + t.subtasks_time_spent) as total_spent,
    CASE 
        WHEN t.subtasks_time_estimate > 0 THEN 'Has Subtasks'
        ELSE 'No Subtasks'
    END as subtask_status
FROM tasks t
WHERE t.subtasks_time_estimate > 0 OR t.subtasks_time_spent > 0
ORDER BY t.subtasks_time_spent DESC;

-- 10. Get tasks with highest subtask time contribution
SELECT 
    t.task_id,
    t.name as task_name,
    t.time_spent as main_task_spent,
    t.subtasks_time_spent,
    ROUND((t.subtasks_time_spent / (t.time_spent + t.subtasks_time_spent)) * 100, 2) as subtask_percentage
FROM tasks t
WHERE (t.time_spent + t.subtasks_time_spent) > 0
ORDER BY subtask_percentage DESC;

-- 11. Get tasks with time discrepancies between main and subtasks
SELECT 
    t.task_id,
    t.name as task_name,
    t.time_estimate as main_estimate,
    t.subtasks_time_estimate as subtasks_estimate,
    t.time_spent as main_spent,
    t.subtasks_time_spent as subtasks_spent,
    (t.time_estimate - t.subtasks_time_estimate) as estimate_difference,
    (t.time_spent - t.subtasks_time_spent) as spent_difference
FROM tasks t
WHERE t.subtasks_time_estimate > 0 OR t.subtasks_time_spent > 0
ORDER BY ABS(t.time_spent - t.subtasks_time_spent) DESC; 