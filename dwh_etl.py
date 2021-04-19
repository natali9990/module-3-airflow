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
all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)
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

    

    dds_hub >> all_hubs_loaded

 
# словарь соответсвия названия линков и набора колонок для вставок, ключей    
link_dict={'payment':["pay_pk,user_pk, billing_period_pk, pay_doc_type_pk, effective_from, load_date, record_source",
                      "a.pay_pk,a.user_pk, a.billing_period_pk, a.pay_doc_type_pk, a.effective_from, a.load_date, a.record_source",
                      "pay"],
           'user_account':["user_account_pk,user_pk, account_pk, load_date, record_source",
                           "a.user_account_pk,a.user_pk, a.account_pk, a.load_date, a.record_source",
                          "user_account"]}
all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)
for i,j in link_dict.items():
    dds_link = PostgresOperator(
    task_id="dds_link_"+i,
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
           with row_rank_1 as (
             select distinct """+j[0]+\
	                    """ from rtk_de.nmezhevova.ods_v_payment 
	                where EXTRACT(year FROM  pay_date)={{ execution_date.year }})
                insert into rtk_de.nmezhevova.dds_link_"""+i+\
                    " select "+j[1]+""" from row_rank_1 as a
	                left join rtk_de.nmezhevova.dds_link_"""+i+""" as tgt
	                on a."""+j[2]+"_pk=tgt."+j[2]+"""_pk
	                where tgt."""+j[2]+"""_pk is null;
                    
        """
    )

    all_hubs_loaded >> dds_link

    

    dds_link >> all_links_loaded
    
    
    
# словарь соответсвия названия саттелитов и набора колонок для вставок, ключей    
sat_dict={'user':["a.user_pk,a.user_hashdiff,a.phone,a.effective_from,a.load_date,a.record_source",
                   "c.user_pk,c.user_hashdiff,c.load_date",
                   "e.user_pk,e.user_hashdiff,e.phone,e.effective_from,e.load_date,e.record_source"],
           'pay':["a.user_pk,a.pay_doc_type_pk,a.pay_hashdiff,a.pay_doc_num,a.sum, a.effective_from,a.load_date,a.record_source",
                  "c.user_pk,c.pay_doc_type_pk,c.pay_hashdiff,c.load_date",
                  "e.user_pk,e.pay_doc_type_pk,e.pay_hashdiff,e.pay_doc_num,e.sum,e.effective_from,e.load_date,e.record_source"]}
all_sat_loaded = DummyOperator(task_id="all_sat_loaded", dag=dag)
for i,j in sat_dict.items():
    dds_sat = PostgresOperator(
        task_id="dds_sat_"+i+"_details",
        dag=dag,
        # postgres_conn_id="postgres_default",
        sql="""
                with source_data as (
		    select """+j[0]+\
		    """ from rtk_de.nmezhevova.ods_v_payment as a
                   where EXTRACT(year FROM  pay_date)={{ execution_date.year }}),
		
		update_records as (
		select """+j[0]+\
		" from rtk_de.nmezhevova.dds_sat_"+i+"""_details as a
		join source_data as b
		on a.user_pk=b.user_pk),
		
		latest_records as (
			select * from (
				select """+j[1]+""",
				case when rank() over (partition by c.user_pk order by c.load_date desc)=1
				then 'Y' else 'N' end as latest
				from update_records as c) as s
				where latest='Y')
		
	     insert into rtk_de.nmezhevova.dds_sat_"""+i+"""_details
	     select distinct """+j[2]+\
		""" from source_data as e
		left join latest_records
		on latest_records."""+i+"_hashdiff=e."+i+"""_hashdiff
		where latest_records."""+i+"""_hashdiff is null;
                    
        """
    )

all_links_loaded >> dds_sat



dds_sat >> all_sat_loaded 
    



