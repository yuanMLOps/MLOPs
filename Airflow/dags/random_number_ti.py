from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime
import random

@dag(
    start_date=datetime(2025, 7, 1),
    schedule="@daily",
    description='A simple DAG to generate and check random numbers',
    catchup=False
)
def random_number_checker_ti():

    @task
    def generate_random_number():
        number = random.randint(1, 100)
        context = get_current_context()
        ti = context["ti"]
        ti.xcom_push(key="number", value=number)
        print(f"generated random number {number}")
        
    
    @task
    def check_even_odd():
        context = get_current_context()
        ti = context["ti"]
        value = ti.xcom_pull(task_ids="generate_random_number", key="number")
        
        result = "even" if value % 2 == 0 else "odd"
        print(f"the number {value} is {result}")

    generate_num_task = generate_random_number()
    check_even_odd_task = check_even_odd()
    generate_num_task >> check_even_odd_task  

random_number_checker_ti()      