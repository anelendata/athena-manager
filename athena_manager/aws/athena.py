#!/usr/bin/env python3
import logging, sys, time
import boto3

from athena_manager.aws import credentials as cred


logger = logging.getLogger(__name__)


def submit_query(query, database, s3_output_location):
    """
    Submit query to Athena
    """
    client = cred.get_client("athena")
    response = client.start_query_execution(
        QueryString=query,
        # ClientRequestToken="string",
        QueryExecutionContext={
            "Database": database,
        },
        ResultConfiguration={
            "OutputLocation": s3_output_location,
            "EncryptionConfiguration": {
                "EncryptionOption": "SSE_S3",
                # "EncryptionOption": "SSE_S3"|"SSE_KMS"|"CSE_KMS",
                # "KmsKey": "string"
            }
        },
        # WorkGroup=workgroup
    )
    return response


def run_query(database, query, workgroup, output_location=None, dry_run=False):
    if dry_run:
        return {"QueryExecutionId": "dry_run",
                "client": client, "query": query, "database": database,
                "s3_bucket": output_location}
    response = submit_query(query, database, output_location)
    logger.info("Submitted query (query_id: %s)" % response["QueryExecutionId"])
    logger.debug("Query: " + query)
    return response


def run_query_and_wait(database, query, workgroup, output_location=None,
                       timeout=600, check_interval=5):
    response = run_query(database, query, workgroup, output_location)
    query_execution_id = response["QueryExecutionId"]

    # Wait for the completion.
    status = "QUEUED"
    logger.info("Waiting for query %s to complete." % query_execution_id)
    current_time = start_time = time.time()
    while status in ("RUNNING", "QUEUED") and current_time - start_time < timeout:
        current_time = time.time()
        sys.stdout.write(".")
        time.sleep(check_interval)
        response = get_query_status(query_execution_id)
        status = response["QueryExecution"]["Status"]["State"]
    if current_time - start_time >= timeout:
        logger.warn("run_query_and_wait: timed out after %d seconds." % timeout)
    return(response)


def get_query_status(query_execution_id):
    """
    Get the query execution status
    """
    client = cred.get_client("athena")
    response = client.get_query_execution(QueryExecutionId=query_execution_id)
    return response

def fetch_result(query_execution_id, page_size=1000):
    """Fetch the result of the completed query
    TODO: Implement an option to fetch only a specified page
    """
    client = cred.get_client("athena")
    results_paginator = client.get_paginator("get_query_results")
    results_iter = results_paginator.paginate(
        QueryExecutionId=query_execution_id,
        PaginationConfig={
            "PageSize": page_size
        }
    )
    results = []
    data_list = []
    for results_page in results_iter:
        for row in results_page["ResultSet"]["Rows"]:
            data_list.append(row["Data"])
    for datum in data_list[0:]:
        results.append([x["VarCharValue"] for x in datum])
    return [tuple(x) for x in results]


def read_query_from_file(filename):
    with open(filename, "r") as f:
        query = f.read().replace("\n", " ")
    return query
