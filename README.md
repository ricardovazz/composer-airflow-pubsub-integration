# composer-airflow-pubsub-integration

## Overview

This repository contains two Apache Airflow Directed Acyclic Graphs (DAGs) designed for triggering workflows based on messages from Google Cloud Pub/Sub. The `trigger_dag` listens for messages on a specified Pub/Sub topic and triggers the `target_dag` based on the content of those messages.

## Prerequisites

- Apache Airflow 2.3.0 or newer
- Access to Google Cloud Pub/Sub
- Google Cloud project ID and credentials

## Configuration

Before deploying these DAGs, ensure the following are configured in your environment:

- **Google Cloud Project ID:** Update the `PROJECT_ID` variable in the script to match your Google Cloud Project ID.
- **Pub/Sub Topic and Subscription:** Set the `TOPIC_ID` and `SUBSCRIPTION` variables to your desired Pub/Sub topic and subscription names, respectively.

## DAG Descriptions

### trigger_dag

- **Purpose:** Listens for messages on a Google Cloud Pub/Sub topic and triggers the `target_dag` based on the messages received.
- **Schedule:** Runs minutely.
- **Key Components:**
  - **PubSubCreateSubscriptionOperator:** Ensures a subscription exists for the specified topic.
  - **PubSubPullOperator:** Pulls messages from the subscription and processes them through a callback function.
  - **TriggerDagRunOperator:** Dynamically triggers `target_dag` based on the messages received.

### target_dag

- **Purpose:** Performs a set of tasks specified by the triggering message from `trigger_dag`.
- **Schedule:** Trigger-only. Does not run on a schedule.
- **Key Components:**
  - **PythonOperator (`some_heavy_task`):** Executes a placeholder task to simulate a workload.
  - **PythonOperator (`acknowledge_pubsub_messages`):** Acknowledges the message in Pub/Sub to prevent reprocessing.

## Setup Instructions

1. **Configure Airflow Environment:** Ensure Airflow is installed and configured in your environment, including connections to Google Cloud.
2. **Deploy DAGs:** Place the Python scripts in your Airflow DAGs folder.
3. **Update Airflow Variables:** Modify the `PROJECT_ID`, `TOPIC_ID`, and `SUBSCRIPTION` variables as needed.
4. **Start Airflow:** Ensure the Airflow scheduler and web server are running.
5. **Trigger `trigger_dag`:** The `trigger_dag` will automatically start running based on its schedule. Alternatively, you can manually trigger it from the Airflow UI.

## Acknowledgments

- These DAGs use features from Apache Airflow 2.3.0, including Dynamic Task Mapping.
- The Pub/Sub integration leverages operators and hooks from Airflow's Google Cloud provider package.

## Disclaimer

This example is for demonstration purposes only. Ensure you test and customize it according to your needs and security practices, especially for production environments.
