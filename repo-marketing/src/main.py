import functions_framework
import pandas_gbq as gbq
from google.cloud import bigquery
from query_strings import func_delete_records_from_bigquery, func_fetch_repo_events
import requests
import json

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

def send_slack_notification(webhook_url, message):
    headers = {
        'Content-Type': 'application/json',
    }
    payload = {
        'text': message,
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        print('Message sent successfully.')
    else:
        print(f'Failed to send message. Status code: {response.status_code}, Response: {response.text}')


@functions_framework.http
def HelloHTTP(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """

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

    webhook_url = 'https://hooks.slack.com/services/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'
    message = ':large_blue_circle: repo_marketing ETL job is done'
    send_slack_notification(webhook_url, message)
