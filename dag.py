from __future__ import annotations

from datetime import datetime
import time

from airflow import DAG
from airflow import XComArg
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
    PubSubPullOperator,
)
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook

PROJECT_ID = "prj-gft-nightwing-0001"
TOPIC_ID = "dag-topic-trigger"
SUBSCRIPTION = "trigger_dag_subscription"


def handle_messages(pulled_messages, context):
    messages = list()
    for idx, m in enumerate(pulled_messages):
        data = m.message.data.decode("utf-8")
        ack_id = m.ack_id
        print(f"message {idx} data is {data}, ack_id : {ack_id}")
        messages.append({"data": data, "ack_id": ack_id})
    return messages
#[
#    {"data": "dag_run_1", "ack_id": "ack12345"},
#]


# This DAG will run minutely and handle pub/sub messages by triggering target DAG
with DAG(
    "trigger_dag",
    start_date=datetime(2021, 1, 1),
    schedule_interval="* * * * *",
    max_active_runs=1,
    catchup=False,
) as trigger_dag:
    # If subscription exists, we will use it. If not - create new one
    subscribe_task = PubSubCreateSubscriptionOperator(
        task_id="subscribe_task",
        project_id=PROJECT_ID,
        topic=TOPIC_ID,
        subscription=SUBSCRIPTION,
    )

    subscription = subscribe_task.output

    # Proceed maximum 50 messages in callback function handle_messages
    # Here we acknowledge messages automatically. You can use PubSubHook.acknowledge to acknowledge in downstream tasks
    # https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/hooks/pubsub/index.html#airflow.providers.google.cloud.hooks.pubsub.PubSubHook.acknowledge
    pull_messages_operator = PubSubPullOperator(
    task_id="pull_messages_operator",
    project_id=PROJECT_ID,
    ack_messages=False,  # Do not acknowledge immediately
    messages_callback=handle_messages,
    subscription=subscription,
    max_messages=50,
    )


    # Here we use Dynamic Task Mapping to trigger DAGs according to messages content
    # https://airflow.apache.org/docs/apache-airflow/2.3.0/concepts/dynamic-task-mapping.html
    trigger_target_dag = TriggerDagRunOperator.partial(task_id="trigger_target", trigger_dag_id="target_dag").expand(  #task is arg for func TriggerDAG
        conf=XComArg(pull_messages_operator) #  calls dag several times with task output # Your logic to determine the DAG ID based on the message, list
    )

    (subscribe_task >> pull_messages_operator >> trigger_target_dag)


def _some_heavy_task():
    print("Do some operation...")
    time.sleep(1)
    print("Done!")

def acknowledge_pubsub_messages(ack_id):
    """
    Acknowledges the messages with the given ack_ids in the specified Pub/Sub subscription.

    :param ack_ids: List of acknowledgment IDs of the messages to acknowledge.
    :param subscription: The subscription from which the messages were received.
    :param project_id: Google Cloud Project ID where the subscription exists.
    """
    # Initialize the hook
    pubsub_hook = PubSubHook()

    # Use the acknowledge method
    pubsub_hook.acknowledge(
        subscription=SUBSCRIPTION,
        ack_ids=[ack_id],
        project_id=PROJECT_ID
    )


# Simple target DAG
with DAG(
    "target_dag",
    start_date=datetime(2022, 1, 1),
    # Not scheduled, trigger only
    schedule_interval=None,
    catchup=False,
) as target_dag:
    some_heavy_task = PythonOperator(
        task_id="some_heavy_task", python_callable=_some_heavy_task
    )

    acknowledge_messages_task = PythonOperator(
        task_id="acknowledge_pubsub_messages",
        python_callable=acknowledge_pubsub_messages,
        op_kwargs={
            "ack_id": "{{ dag_run.conf['ack_id'] }}",
        },
    )

    (some_heavy_task >> acknowledge_messages_task)