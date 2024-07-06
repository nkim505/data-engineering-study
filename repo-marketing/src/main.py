# url: https://asia-northeast3-refined-asset-427901-s7.cloudfunctions.net/update_repo_marketing
# 트리거 유형 https
import functions_framework
import pandas_gbq as gbq
from google.oauth2 import service_account
from google.cloud import bigquery
from query_strings import func_delete_records_from_bigquery, func_fetch_repo_events


def _load_df_from_bigquery(client, query):
    query_job = client.query(query)
    return query_job.to_dataframe()


def extract_github_raw_data(client, target_date):
    print(f"extracting..")
    q = func_fetch_repo_events(target_date)
    result_df = _load_df_from_bigquery(client, q)
    return result_df


def delete_records_from_bigquery(client, project_id, dataset_id, table_id, target_date):

    print("deleting...")
    q = func_delete_records_from_bigquery(project_id, dataset_id, table_id, target_date)

    try:
        client.query(q).result()
        print(f"{target_date} records has deleted from bigquery ")
    except Exception as e:
        print(e)


def load_dataframe_to_bigquery(target_date, df, project_id, dataset_id, table_id):
    print("loading...")
    gbq.to_gbq(
        df,
        f"{project_id}.{dataset_id}.{table_id}",
        project_id=project_id,
        if_exists="append",
    )
    print(f"{target_date} Data loaded to `{project_id}.{dataset_id}.{table_id}`")


@functions_framework.http
def etl_repo_marketing(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """

    # 로컬에서 살리기
    # SERVICE_ACCOUNT_FILE = "../configs/bigquery_key.json"  # 키 json 파일
    # credentials = service_account.Credentials.from_service_account_file(
    #     SERVICE_ACCOUNT_FILE
    # )

    project_id = "data-427901"
    client = bigquery.Client(project=project_id)

    request_json = request.get_json(silent=True)
    target_date = request_json["target_date"]
    print(f"target_date: {target_date}")

    extracted_df = extract_github_raw_data(client, target_date)
    delete_records_from_bigquery(
        client, project_id, "repo_marketing", "fact_repo_events", target_date
    )
    load_dataframe_to_bigquery(
        target_date,
        extracted_df,
        project_id,
        "repo_marketing",
        "fact_repo_events",
    )
