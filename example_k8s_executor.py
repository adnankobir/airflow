import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
import os

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='example_kubernetes_executor', default_args=args,
    schedule_interval=None
)

affinity = {
    'podAntiAffinity': {
        'requiredDuringSchedulingIgnoredDuringExecution': [
            {
                'topologyKey': 'kubernetes.io/hostname',
                'labelSelector': {
                    'matchExpressions': [
                        {
                            'key': 'app',
                            'operator': 'In',
                            'values': ['airflow']
                        }
                    ]
                }
            }
        ]
    }
}

tolerations = [{
    'key': 'dedicated',
    'operator': 'Equal',
    'value': 'airflow'
}]


def print_stuff():
    print("stuff!")


def use_airflow_binary():
    rc = os.system("airflow -h")
    assert rc == 0

# You don't have to use any special KubernetesExecutor configuration if you don't want to
start_task = PythonOperator(
    task_id="start_task", python_callable=print_stuff, dag=dag
)

# But you can if you want to
one_task = PythonOperator(
    task_id="one_task", python_callable=print_stuff, dag=dag,
    executor_config={"KubernetesExecutor": {"image": "apache/airflow:master-ci-slim"}}
)

# Use the airflow -h binary
two_task = PythonOperator(
    task_id="two_task", python_callable=use_airflow_binary, dag=dag,
    executor_config={"KubernetesExecutor": {"image": "apache/airflow:master-ci-slim"}}
)

# Limit resources on this operator/task with node affinity & tolerations
three_task = PythonOperator(
    task_id="three_task", python_callable=print_stuff, dag=dag,
    executor_config={
        "KubernetesExecutor": {"request_memory": "128Mi",
                               "limit_memory": "128Mi",
                               "tolerations": tolerations,
                               "affinity": affinity}}
)

# Add arbitrary labels to worker pods
four_task = PythonOperator(
    task_id="four_task", python_callable=print_stuff, dag=dag,
    executor_config={"KubernetesExecutor": {"labels": {"foo": "bar"}}}
)

start_task.set_downstream([one_task, two_task, three_task, four_task])
