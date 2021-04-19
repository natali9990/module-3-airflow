from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'nmezhevova'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)
# список сущностей для хабов
hub_lst=['user','account','pay_doc_type','billing_period']
for i in hub_lst:
    dds_hub = PostgresOperator(
        task_id="dds_hub_"+i,
        dag=dag,
        # postgres_conn_id="postgres_default",
        sql="""with row_rank_1 as (
                select * from (
		            select """+i+"_pk,"+i+"""_key,load_date,record_source,
			            row_number() over(
			            partition by """+i+"""_pk
			            order by load_date asc) as row_number 
			        from rtk_de.nmezhevova.ods_v_payment
                    where EXTRACT(year FROM  pay_date)={{ execution_date.year }}) as h
		        where row_number = 1)
            insert into rtk_de.nmezhevova.dds_hub_"""+i+\
	            " select a."+i+"_pk,a."+i+"""_key,a.load_date,a.record_source
	            from row_rank_1 as a
	            left join rtk_de.nmezhevova.dds_hub_"""+i+""" as d
	            on a."""+i+"_pk=d."+i+"""_pk
	            where d."""+i+"""_pk is null

        """
    )

    all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

    dds_hub >> all_hubs_loaded

dds_link_user_payment = PostgresOperator(
    task_id="dds_link_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
    with row_rank_1 as (
select distinct pay_pk,user_pk, billing_period_pk, pay_doc_type_pk, effective_from, load_date, record_source
	from rtk_de.nmezhevova.ods_v_payment 
	where EXTRACT(year FROM  pay_date)=2013)
insert into rtk_de.nmezhevova.dds_link_payment
select a.pay_pk,a.user_pk, a.billing_period_pk, a.pay_doc_type_pk, a.effective_from, a.load_date, a.record_source from row_rank_1 as a
	left join rtk_de.nmezhevova.dds_link_payment as tgt
	on a.pay_pk=tgt.pay_pk
	where tgt.pay_pk is null;
"""
all_hubs_loaded >> dds_link_payment

all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

dds_link_payment >> all_links_loaded

