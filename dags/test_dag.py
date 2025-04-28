from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Функция, которую будет выполнять PythonOperator
def my_task_function(**kwargs):
    print("Hello from my task!")

# Определение DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

dag = DAG(
    'simple_example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval='@daily',  # Запускать каждый день
)

# Определение задач в DAG
start = DummyOperator(
    task_id='start',
    dag=dag,
)

task1 = PythonOperator(
    task_id='my_task',
    python_callable=my_task_function,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Определение порядка выполнения задач
start >> task1 >> end
