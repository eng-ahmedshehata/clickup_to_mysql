USE clickup_sec_try;
USE clickup_ss;

ALTER TABLE time_entries DROP PRIMARY KEY, ADD PRIMARY KEY (time_entry_id, task_id, user_id);

ALTER TABLE time_entries DROP PRIMARY KEY, ADD PRIMARY KEY (time_entry_id, task_id, user_id);

DELETE FROM spaces WHERE space_id IS NULL;

DROP TABLE IF EXISTS lists;
DROP TABLE IF EXISTS folders;
DROP TABLE IF EXISTS spaces;
DROP TABLE IF EXISTS tasks;

select * from spaces;
select * from folders;
select * from lists;
select * from lists where sprint_check=true;
select * from sprints;
select * from task_sprint_link;
select * from time_entries;
select * from task_time_in_status;
SELECT time_entry_id FROM time_entries;
select * from tasks;
select * from tasks where list_id='901512722155';
