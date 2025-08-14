USE new;
USE clickup_sec_try;
USE clickup_ss;
USE clickup_3rd_try;

ALTER TABLE time_entries DROP PRIMARY KEY, ADD PRIMARY KEY (time_entry_id, task_id, user_id);

ALTER TABLE time_entries DROP PRIMARY KEY, ADD PRIMARY KEY (time_entry_id, task_id, user_id);

DELETE FROM spaces WHERE space_id IS NULL;


DROP TABLE IF EXISTS spaces;
DROP TABLE IF EXISTS folders;
DROP TABLE IF EXISTS lists;
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS relations;
DROP TABLE IF EXISTS sprints;
DROP TABLE IF EXISTS subtasks;
DROP TABLE IF EXISTS task_time_in_status;


select * from spaces;
select * from folders;
select * from lists;
select * from lists where sprint_check=true;
select * from sprints;
select * from sprints where linked_taskstask_id='86c4r2ytg';
-- select * from task_sprint_link;
-- select * from time_entries;
select * from task_time_in_status;
-- SELECT time_entry_id FROM time_entries;
select * from tasks;
select * from tasks where task_id='86c2zjkzt';
select * from tasks where list_id='901508047481';
select * from tasks where custom_item_id='0' and list_id='901508047481';
select * from tasks where custom_item_id='1002';
select task_id, name , us_cycle_time , date_done from tasks where task_id='86c2zjkzt';
select * from subtasks;
select * from subtasks where task_id='86c211gdg';
select * from task_time_in_status;
select * from task_time_in_status where task_id='86c2zjkzt';

SELECT 
    s.task_name,s.subtask_name,s.description,s.status
FROM 
    subtasks AS s
INNER JOIN 
    tasks AS t
  ON s.task_id = t.task_id
INNER JOIN 
    lists AS l
  ON t.list_id = l.list_id
WHERE 
    t.custom_item_id = '0'     -- replace with your desired custom_field_id
  AND 
    l.list_id = '901508047481';