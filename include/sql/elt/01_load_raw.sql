CREATE TABLE IF NOT EXISTS {{table_name}} AS
SELECT *, CAST(NULL AS TIMESTAMP) AS _loaded_at,
       CAST(NULL AS VARCHAR) AS _source_file,
       CAST(NULL AS VARCHAR) AS _run_id
FROM read_csv('{{path}}', header = true, all_varchar = true)
WHERE FALSE;

INSERT INTO {{table_name}}
SELECT *, CURRENT_TIMESTAMP AS _loaded_at,
       '{{source_file}}' AS _source_file,
       '{{run_id}}' AS _run_id
FROM read_csv('{{path}}', header = true, all_varchar = true);
