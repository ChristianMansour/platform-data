from datetime import datetime
from airflow.sdk import dag, task
import requests


def notify_failure(context):
    import requests
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    message = f" Échec Airflow\nDAG: {dag_id}\nTask: {task_id}\nRun: {run_id}"
    requests.post(
        "https://ntfy.sh/airflow-platform-data-notifications",
        data=message.encode('utf-8')
    )


@dag(
    dag_id="test_notification",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test"],
)
def test_notification_dag():

    @task(
        retries=1,
        on_failure_callback=notify_failure,
    )
    def fail_on_purpose():
        raise ValueError("Échec volontaire pour tester la notification")

    fail_on_purpose()


test_dag = test_notification_dag()
