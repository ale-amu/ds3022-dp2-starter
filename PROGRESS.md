# Progress log

Add a new entry at the end of every session (and whenever you stop working).

## Mon 05 Oct (Lesson 9)
- Done: finished SETUP instructions/tasks: focused on checkpoint A's populate_queue, get_counts and monitor_queue. Added Checkpoint A's screenshot in docs.
- Next: Start Checkpoint B (collect_messages: receive, insert into DuckDB, then delete).
- Blocked: Write polling strategy in README. 


## Thu 08 Oct (Lesson 10)
- Done: finished Checkpoint B 
and Checkpoint C (focused on stg_fragments, schema.yml tests,... dbt_build gate; and added the fail/pass screenshots in docs folder). Finished Checkpoint D and added receipt to README). finished DAG image and saved as dag.jpg in docs.
- Next: Checkpoint F: full-delay and working on README
- Blocked: Optional Checkpoint E.


## Mon 19 Oct (Lesson 11)
- Done: Checkpoint F. Chaos mode: first chaos run crashed because of KeyError 'word' on the malformed message; fixed collect_messages to log and delete malformed messages and to keep receiving until the queue is empty. Two consecutive runs in a row ended in PASS. A full-delay run (DP2_FAST=0) was completed in 14m 27s with the screenshot saved in docs as checkpointF_full-delay-run.png.
- Next: finish README, fill in RUBRIC.md self-assessment, commit and push, submit by 23:59.
- Blocked: N/A


## Tue 20 Oct
- Done: finished README (made a section for each rubric item, and commited all screenshots), named screenshots by checkpoint and pushed README and docs to GitHub and fill in RUBRIC.md self-assessment
- Next: N/A
- Blocked: N/A
