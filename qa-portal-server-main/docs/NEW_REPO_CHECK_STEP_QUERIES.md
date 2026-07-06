# New Repo Check Step Queries

This note summarizes the step-by-step SQL and file layout used by:

- `POST /api/v2/rts/check/run-repo-new`
- `POST /api/v2/rts/check/run-repo-new-job`

The implementation lives in `app/services/new_repo_check_service.py`.

## UI and Input Routing

- `GET /api/v2/rts/check/repo-status` returns `engine` and `schema_name`.
- `app/templates/rts_check.html` uses those values to branch the Step 5 input UI.
- For PostgreSQL repo checks, `${schema_name}` is injected into `app/sql_templates/pg_step5.txt`.

## Runtime SQL Template Files

- `app/sql_templates/step2.txt`
- `app/sql_templates/step3.txt`
- `app/sql_templates/step4.txt`
- `app/sql_templates/ora_step5.txt`
- `app/sql_templates/pg_step5.txt`
- `app/sql_templates/step6.txt`

If one of these files is missing, `new_repo_check_service.py` falls back to inline SQL for that step.

## Step Summary

1. Step 1 checks required target privileges such as `ALTER SYSTEM`, `DBMS_LOCK`, and `DBMS_UTILITY`.
2. Step 2 creates the test procedures, primarily from `app/sql_templates/step2.txt`.
3. Step 3 runs the PL/SQL loop workload, primarily from `app/sql_templates/step3.txt`.
4. Step 4 collects target `v$sql` evidence, primarily from `app/sql_templates/step4.txt`.
5. Step 5 validates repo-side collected rows using:
   - `app/sql_templates/ora_step5.txt` for Oracle repo
   - `app/sql_templates/pg_step5.txt` for PostgreSQL repo
6. Step 6 purges and cleans up test objects, primarily from `app/sql_templates/step6.txt`.
