def func_fetch_repo_events(target_date):

    date_string = target_date.replace("-", "")

    return f"""
    SELECT
    DATE(TIMESTAMP(created_at), 'Asia/Seoul') as event_date_kst
        ,repo.id
        , type as metric
        , count(created_at) as value
    FROM `githubarchive.day.{date_string}`
    WHERE 1=1
      AND repo.name like '%openai/openai%'
      AND RIGHT(id, 2) in ("13", "29", "33", "97") ## 4%만 systematic sampling
    group by all
"""


def func_delete_records_from_bigquery(project_id, dataset_id, table_id, target_date):
    return f"""
    DELETE
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE event_date_kst = '{target_date}'
"""
