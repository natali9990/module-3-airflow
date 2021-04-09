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
insert_dict={'ods_billing':["user_id, billing_period, service, tariff, cast(sum as INT), cast(created_at as DATE),'created_at','stg_billing'],
             'ods_issue':["cast(user_id as INT), cast(start_time as TIMESTAMP), cast(end_time as TIMESTAMP), title, description, service",'start_time','stg_issue'],
             'ods_payment':["user_id, pay_doc_type, pay_doc_num, account,phone,billing_period,cast(pay_date as DATE),cast(sum as INT)",'pay_date','stg_payment'],
             'ods_traffic':["user_id,cast(`timestamp` as TIMESTAMP),device_id, device_ip_addr, bytes_sent,bytes_received",'cast(`timestamp` as TIMESTAMP)','stg_traffic'],
             'dm_user_traffic':["user_id, max(bytes_received),min(bytes_received),avg(bytes_received)",'event','ods_traffic']}

for i,j in insert_dict.items():            
   inquiry='insert overwrite table nmezhevova.'+i+" partition (year='{{ execution_date.year }}') select "+ \
   j[0]+'from nmezhevova.'+j[2]+' where year('+j[1]+') = {{ execution_date.year }}'+(' GROUP BY user_id' if i=='dm_user_traffic' else '')+';'
   ods_table = DataProcHiveOperator(
   task_id=i,
   dag=dag,
   query=inquiry,            
   cluster_name='cluster-dataproc',
   job_name=USERNAME + '_'+i+'_{{ execution_date.year }}_{{ params.job_suffix }}',
   params={"job_suffix": randint(0, 100000)},
   region='europe-west3',
   )
    
