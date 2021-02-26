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
dict={"":{"rocket": "all"},
      " -r falcon1":{"rocket": "falcon1"},
      " -r falcon9":{"rocket": "falcon9"},
      " -r falconheavy":{"rocket": "falconheavy"}}
for i,j in dict.items():
    comm1="python3 /root/airflow/dags/spacex/load_launches.py -y {{ execution_date.year }} -o /var/data"+i
    name1="get_data_"+j["rocket"]
    name2="print_data_"+j["rocket"]
    t1 = BashOperator(
    task_id=name1, 
    bash_command=comm1, 
    dag=dag
    )
    t2 = BashOperator(
    task_id=name2, 
    bash_command="cat /var/data/year={{ execution_date.year }}/rocket={{params.rocket}}/data.csv", 
    params=j, # falcon1/falcon9/falconheavy
    dag=dag
    )
    t1 >> t2
