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
    USERNAME + '_dwh_etl_final',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)

ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)
all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)
all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)
all_sat_loaded = DummyOperator(task_id="all_sat_loaded", dag=dag)

sources=['payment','billing','issue','traffic']
for i in sources:
    clear_ods = PostgresOperator(
        task_id=fr"clear_ods_{i}",
	dag=dag,        
	sql=fr"""
	DELETE FROM nmezhevova.ods_{i} where EXTRACT(year FROM  pay_date::DATE)={{ execution_date.year }}
	"""
	)
    fill_ods = PostgresOperator(
        task_id=fr"fill_ods_{i}",
	dag=dag,        
	sql=fr"""
	INSERT INTO nmezhevova.ods_{i} 
	SELECT * FROM nmezhevova.stg_{i}
	where EXTRACT(year FROM  pay_date::DATE)={{ execution_date.year }}
	"""
	)    
    clear_ods>>fill_ods>>ods_loaded


# список сущностей для хабов
hub_lst={'payment':['user','account','pay_doc_type','billing_period'],'billing':['service','tariff'],'traffic':'device_id'}
for j in hub_lst:
    for i in hub_lst[i]:
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
			        from rtk_de.nmezhevova.ods_v_"""+j+\
                    """ where EXTRACT(year FROM  pay_date)={{ execution_date.year }}) as h
		        where row_number = 1)
            insert into rtk_de.nmezhevova.dds_hub_"""+i+\
	            " select a."+i+"_pk,a."+i+"""_key,a.load_date,a.record_source
	            from row_rank_1 as a
	            left join rtk_de.nmezhevova.dds_hub_"""+i+""" as d
	            on a."""+i+"_pk=d."+i+"""_pk
	            where d."""+i+"""_pk is null
        	"""
    	)

    
    ods_loaded>>dds_hub
    dds_hub >> all_hubs_loaded

 
# словарь соответсвия названия линков и набора колонок для вставок, ключей    
link_dict={'payment':["pay_pk,user_pk, billing_period_pk, pay_doc_type_pk, effective_from, load_date,sum, record_source",		      
                      "a.pay_pk,a.user_pk, a.billing_period_pk, a.pay_doc_type_pk, a.effective_from, a.load_date,a.sum, a.record_source",
                      "pay"],
           'user_account':["user_account_pk,user_pk, account_pk, load_date, record_source",
                           "a.user_account_pk,a.user_pk, a.account_pk, a.load_date, a.record_source",
                          "user_account"],
	  'user_billing_period_service_tariff':["user_billing_period_service_tariff_pk,user_pk, billing_period_pk,service_pk,tariff_pk, effective_from, load_date,sum, record_source",
						"a.user_billing_period_service_tariff_pk,a.user_pk, a.billing_period_pk,a.service_pk,a.tariff_pk, a.effective_from, a.load_date,sum, a.record_source",
					       "user_billing_period_service_tariff_pk"],
	  'user_service':["user_service_pk,user_pk, service_pk,start_time, end_time, load_date,  record_source",
			  "a.user_service_pk,a.user_pk, a.service_pk,a.start_time, a.end_time, a.load_date,  a.record_source","user_service_pk"],
	  'user_device':["user_device_pk,user_pk, device_id_pk,effective_from, bytes_sent,bytes_received, load_date, record_source",
			 "a.user_device_pk,a.user_pk, a.device_id_pk,a.effective_from, a.bytes_sent,a.bytes_received, a.load_date, a.record_source","user_device_pk"]}

for i,j in link_dict.items():
    if i=='payment' or i=='user_account':
	ods_tabl='payment'
    elif i=='user_billing_period_service_tariff':
	ods_tabl='billing'
    elif i== 'user_service':
	ods_tabl='issue'
    elif i== 'user_device':
	ods_tabl='traffic'
    dds_link = PostgresOperator(
    task_id="dds_link_"+i,
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
           with row_rank_1 as (
             select distinct """+j[0]+\
	                    " from rtk_de.nmezhevova.ods_v_"+ ods_tabl+
	                """ where EXTRACT(year FROM  pay_date)={{ execution_date.year }})
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
                  "e.user_pk,e.pay_doc_type_pk,e.pay_hashdiff,e.pay_doc_num,e.sum,e.effective_from,e.load_date,e.record_source"],
	 'service':["a.user_service_pk,a.user_pk,a.service_pk,a.service_hashdiff,a.description,a.title,a.start_time,a.end_time,a.load_date,a.record_source",
		    "c.user_service_pk,c.user_pk,c.service_pk,c.service_hashdiff,c.load_date",
		    "e.user_service_pk,e.user_pk,e.service_pk,e.service_hashdiff,e.description,e.title,e.start_time,e.end_time,e.load_date,e.record_source"],
	 'device':["a.device_id_pk,a.device_hashdiff,a.device_ip_addr,a.effective_from,a.load_date,a.record_source",
		   "c.device_id_pk,c.device_hashdiff,c.load_date",
		   "e.device_id_pk,e.device_hashdiff,e.device_ip_addr,e.effective_from,e.load_date,e.record_source"],
	 'mdm':["a.user_pk,a.mdm_hashdiff,a.user_key,a.legal_type,a.district,a.registered_at,a.billing_mode,a.is_vip,a.load_date,a.record_source",
		"c.user_pk,c.mdm_hashdiff,c.load_date",
		"e.user_pk,e.mdm_hashdiff,e.user_key,e.legal_type,e.district,e.registered_at,e.billing_mode,e.is_vip,e.load_date,e.record_source"]}

for i,j in sat_dict.items():
    if i=='user' or i=='pay':
	ods_tabl1='payment'
	ods_key='user_pk'
    elif i=='service':
	ods_tabl1='issue'
	ods_key="user_service_pk"
    elif i== 'device':
	ods_tabl1='traffic'
	ods_key='device_id_pk'
    elif i== 'mdm':
	ods_tabl1='mdm'
	ods_key='user_pk'
    dds_sat = PostgresOperator(
        task_id="dds_sat_"+i+"_details",
        dag=dag,
        # postgres_conn_id="postgres_default",
        sql="""
                with source_data as (
		    select """+j[0]+\
		    " from rtk_de.nmezhevova.ods_v_"+ods_tabl1+""" as a
                   where EXTRACT(year FROM  pay_date)={{ execution_date.year }}),
		
		update_records as (
		select """+j[0]+\
		" from rtk_de.nmezhevova.dds_sat_"+i+"""_details as a
		join source_data as b
		on a."""+ods_key+"=b."+ods_key+"""),
		
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
    

