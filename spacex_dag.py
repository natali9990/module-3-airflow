from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2005, 1, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("spacex", default_args=default_args, schedule_interval="0 0 1 1 *")
dict={"python3 /root/airflow/dags/spacex/load_launches.py -y {{ execution_date.year }} -o /var/data":{"rocket": "all"},
      "python3 /root/airflow/dags/spacex/load_launches.py -y {{ execution_date.year }} -o /var/data -r falcon1":{"rocket": "falcon1"},
      "python3 /root/airflow/dags/spacex/load_launches.py -y {{ execution_date.year }} -o /var/data -r falcon9":{"rocket": "falcon9"},
      "python3 /root/airflow/dags/spacex/load_launches.py -y {{ execution_date.year }} -o /var/data -r falconheavy":{"rocket": "falconheavy"}}
for i,j in dict.items():
    t1 = BashOperator(
    task_id="get_data", 
    bash_command=i, 
    dag=dag
    )
    t2 = BashOperator(
    task_id="print_data", 
    bash_command="cat /var/data/year={{ execution_date.year }}/rocket={{params.rocket}}/data.csv", 
    params=j, # falcon1/falcon9/falconheavy
    dag=dag
    )
    t1 >> t2
