from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'nmezhevova'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_data_lake_etl',
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
)
insert_dict={'billing':["user_id, billing_period, service, tariff, cast(sum as INT), cast(created_at as DATE)",'created_at'],
             'issue':["cast(user_id as INT), cast(start_time as TIMESTAMP), cast(end_time as TIMESTAMP), title, description, service",'start_time'],
             'payment':["user_id, pay_doc_type, pay_doc_num, account,phone,billing_period,cast(pay_date as DATE),cast(sum as INT)",'pay_date'],
             'traffic':["user_id,cast(`timestamp` as TIMESTAMP),device_id, device_ip_addr, bytes_sent,bytes_received",'cast(`timestamp` as TIMESTAMP)'],
             'dm_user_traffic':["user_id, max(bytes_received),min(bytes_received),avg(bytes_received)",'event']}

for i,j in insert_dict.items():
    if i!='dm_user_traffic':
        name_task='ods_'+i
        inquiry=f"""insert overwrite table nmezhevova.ods_{i} partition (year='{{ execution_date.year }}')\ 
            select {j[0]} from nmezhevova.stg_{i} where year({j[1]}) = {{ execution_date.year }};"""
        ods_table = DataProcHiveOperator(
            task_id=name_task,
            dag=dag,
            query=inquiry,            
            cluster_name='cluster-dataproc',
            job_name=USERNAME + f'_ods_{i}_{{ execution_date.year }}_{{ params.job_suffix }}',
            params={"job_suffix": randint(0, 100000)},
            region='europe-west3',
        )
    else:
        dm_table = DataProcHiveOperator(
            task_id=i,
            dag=dag,
            query=f"""
            insert overwrite table nmezhevova.{i} partition (year='{{ execution_date.year }}') 
            select {j[0]} from nmezhevova.ods_traffic where year({j[1]}) = {{ execution_date.year }} GROUP BY user_id;
            """,            
            cluster_name='cluster-dataproc',
            job_name=USERNAME + f'_{i}_{{ execution_date.year }}_{{ params.job_suffix }}',
            params={"job_suffix": randint(0, 100000)},
            region='europe-west3',
        )
