import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from time import sleep
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
    sleep(120)

def print_stuff1():
    print("stuff1!")
    sleep(240)

def print_stuff2():
    print("stuff2!")
    sleep(480)

# You don't have to use any special KubernetesExecutor configuration if you don't want to
start_task = PythonOperator(
    task_id="start_task", python_callable=print_stuff, dag=dag
)

# But you can if you want to
one_task = PythonOperator(
    task_id="one_task", python_callable=print_stuff1, dag=dag
)

# Use the airflow -h binary
two_task = PythonOperator(
    task_id="two_task", python_callable=print_stuff2, dag=dag
)

# Add arbitrary labels to worker pods
four_task = PythonOperator(
    task_id="four_task", python_callable=print_stuff2, dag=dag,
    executor_config={"KubernetesExecutor": {"labels": {"foo": "bar"}}}
)

start_task.set_downstream([one_task, two_task, three_task])
