"""DS 3022 Data Project 2: Prefect flow.

Your pipeline must, without human intervention:
  1. POST to the scatter API to populate your queue (once per run, never inside a retry loop)
  2. Monitor the queue (get_queue_attributes) and collect all 21 messages
  3. Land each fragment in DuckDB (raw.fragments) BEFORE deleting its message
  4. Run dbt: tests are the quality gate, the mart model holds the phrase
  5. Submit the phrase to the 'dp2-submit' queue and log the HTTP status code

Run:   python prefect-flow.py
Then:  python kit/check_submission.py

Every task already has a header. Checkpoint A (Lesson 9): fill in populate_queue,
get_counts and monitor_queue. Lesson 10: collect_messages, dbt_build, read_phrase
and submit_solution. The tasks not called by the flow yet cannot fail, so you can
run the file at any point. The skeleton is a suggestion: rename, split or add
tasks as your design needs, and keep your DAG sketch in step with them.
Use Prefect's logger (get_run_logger) for logging. Never hard-code your
computing ID or the endpoint: read them from the environment (.env).
"""
import os
import subprocess
import time
from datetime import datetime, timezone

import boto3
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
import duckdb

load_dotenv()

UVA_ID = os.environ["UVA_ID"]
SCATTER_URL = f"http://127.0.0.1:{os.environ.get('SCATTER_PORT', '8000')}/api/scatter/{UVA_ID}"
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")

sqs = boto3.client("sqs")  # the endpoint comes from AWS_ENDPOINT_URL_SQS


#populate queue asks scatter api to populate the queue and returns the queue URL as a string

@task #marks fucntion as a prefect task so its its own step
def populate_queue() -> str: #fuction that returns queue URL as a string
    """POST to the scatter API and return your queue URL."""
    logger = get_run_logger() 
    # (Checkpoint A) 
    url = SCATTER_URL
    payload= requests.post(url).json() # sends https post request to scatter api
    queue_url = payload["sqs_url"] # this is the queue URL returned by the scatter API
    logger.info(f"Queue URL: {queue_url}")
    return queue_url


#asks code how many messages are in the queue, how many are not visible, and how many are delayed

def get_counts(queue_url: str) -> dict:
    """Plain helper (not a task): return the three ApproximateNumberOf... counters as integers."""
    # (Checkpoint A)
    url = queue_url # the queue URL returned by the scatter API
    response = sqs.get_queue_attributes(
        QueueUrl=url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"]   
    ) # the 3 counters we want to get from the queue

    attrs = response["Attributes"] 
    counts = {
        "visible": int(attrs.get("ApproximateNumberOfMessages", 0)), 
        "not_visible": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
    }
    return counts

#keep checking queue counts every poll_s seconds until there are no delayed messages, or raise a TimeoutError if timeout_s is exceeded

@task
def monitor_queue(queue_url: str, poll_s: int = 5, timeout_s: int = 1200) -> dict: 
    """Log the three counters every poll_s seconds until nothing is delayed; raise after timeout_s."""
    # (Checkpoint A)
    logger = get_run_logger()
    start= time.time() # tracks the start time of the monitoring
    while True:
        counts = get_counts(queue_url) # get the counts of the queue
        logger.info(f"Queue counts: {counts}") # log the counts
        if counts["delayed"] == 0: # if there are no delayed messages, break the loop
            break
        if time.time() - start > timeout_s: # if the timeout is exceeded, raise an error
            raise TimeoutError(f"Timeout exceeded: {timeout_s} seconds")
        time.sleep(poll_s) # wait for poll_s seconds before checking again
    return counts


# pick up each message, save its number and word in duckdb then delete from queue. save before so its not lost

@task
def collect_messages(queue_url: str, expected: int = 21, timeout_s: int = 300) -> int:
    """Receive, land in DuckDB, then delete. Return how many fragments are stored."""
    # (Checkpoint B): MessageAttributeNames=["All"] and resp.get("Messages", [])
    logger = get_run_logger() # create a log for tracking messages
    logger.info(f"Collecting messages from queue: {queue_url}")
    #collect messages recieves until 21 fragmenst stored
    start = time.time() # tracks the start time of collecting

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE TABLE IF NOT EXISTS raw.fragments (order_no VARCHAR, word VARCHAR, received_at TIMESTAMP)") # varchar means variable length string, timestamp is date and time
    con.execute("DELETE FROM raw.fragments") # every run starts with an empty table

    stored = 0
    # store all 21 then make sure queue is empty 
    while stored < expected or sum(get_counts(queue_url).values()) > 0:
        if time.time() - start > timeout_s: # if timeout is exceeded, it fail 
            con.close()
            raise TimeoutError(f"Timeout exceeded: only {stored}/{expected} fragments stored after {timeout_s} seconds")

        resp = sqs.receive_message( # receive messages from the queue
            QueueUrl=queue_url,     
            MaxNumberOfMessages=10, 
            WaitTimeSeconds=10,
            MessageAttributeNames=["All"], # allows us to get the message attributes (order_no and word) from the messages
        )
        for msg in resp.get("Messages", []): # no 'Messages' key when nothing is visible
            attrs = msg.get("MessageAttributes", {})
            order_no = attrs.get("order_no", {}).get("StringValue")
            word = attrs.get("word", {}).get("StringValue")
            if order_no is None or word is None or not order_no.isdigit():
                # malformed message: log it and delete it, but do not store it
                logger.warning(f"Malformed message, deleting it: {attrs}")
            else:
                # first store fragment, then delete the message 
                con.execute("INSERT INTO raw.fragments VALUES (?, ?, ?)", [order_no, word, datetime.now(timezone.utc)])
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

        stored = con.execute("SELECT count(DISTINCT order_no) FROM raw.fragments").fetchone()[0]
        logger.info(f"Stored {stored}/{expected} fragments")

    con.close() 
    logger.info(f"Final queue counts: {get_counts(queue_url)}") 
    return stored


@task
def dbt_build() -> None:
    """Run `dbt build --project-dir dbt --profiles-dir dbt`; raise if anything fails."""
    # (Checkpoint C)
    logger = get_run_logger()  
    result = subprocess.run( # creates a subprocess which allows us to run dbt build command in the shell
        ["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt"],   
        capture_output=True, text=True,
    )
    logger.info(result.stdout) # shows dbt output in prefect logs
    if result.returncode != 0: # failed test or model stops the flow before submission
        raise RuntimeError("dbt build failed, not submitting")


@task
def read_phrase() -> str:
    """Read the phrase from the dbt mart model in DuckDB."""
    # (Checkpoint C)
    logger = get_run_logger()
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    phrase = con.execute("SELECT phrase FROM main.assembled_phrase").fetchone()[0] # fetchone() returns a tuple, we want the first element which is the phrase
    con.close()
    logger.info(f"Phrase: {phrase}")
    return phrase


@task
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    """Send to dp2-submit with uvaid / phrase / platform attributes. Return the HTTP status code."""
    # (Checkpoint D)
    logger = get_run_logger()
    url = sqs.get_queue_url(QueueName="dp2-submit")["QueueUrl"]
    response = sqs.send_message(
        QueueUrl=url,
        MessageBody="dp2 solution",
        MessageAttributes={
            "uvaid":    {"DataType": "String", "StringValue": UVA_ID},
            "phrase":   {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform},
        },
    )
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    logger.info(f"Submission HTTP status: {status}")
    if status != 200: # makes the run fail if submission was not accepted
        raise RuntimeError(f"Submission failed with HTTP status {status}")
    return status


@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline():
    queue_url = populate_queue()
    monitor_queue(queue_url)
    collect_messages(queue_url)
    dbt_build()
    phrase = read_phrase()
    submit_solution(phrase)


if __name__ == "__main__":
    dp2_pipeline()
