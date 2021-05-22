from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'nmezhevova'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2010, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl_final',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)
all_ods_clear = DummyOperator(task_id="all_ods_clear", dag=dag)
all_ods_loaded = DummyOperator(task_id="all_ods_loaded", dag=dag)
all_view_create = DummyOperator(task_id="all_view_create", dag=dag)
all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)
all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)
all_sat_loaded = DummyOperator(task_id="all_sat_loaded", dag=dag)
all_view_drop= DummyOperator(task_id="all_view_drop", dag=dag)

sources={'payment':"user_id,pay_doc_type,pay_doc_num, account,phone,billing_period,pay_date,cast(sum as decimal)",
	 'billing':"user_id, billing_period,service, tariff, cast(sum as decimal), created_at",
	 'issue':"cast(user_id as INT), start_time,end_time, service, description, title",
	 'traffic':"user_id,to_timestamp(timestamp/1000),device_id, device_ip_addr, bytes_sent,bytes_received",
	'mdm':"*"}
for i,j in sources.items():
    if i=='payment':
        col_date=['pay_date','pay_date']
    elif i=='billing':
        col_date=['created_at','created_at']
    elif i=='issue':
        col_date=['start_time','start_time']
    elif i=='traffic':
        col_date=['event','to_timestamp(timestamp/1000)']
    elif i=='mdm':
        col_date=['registered_at','registered_at']
    clear_ods = PostgresOperator(
    task_id="clear_ods_"+i,
    dag=dag,        
    sql="""
    DELETE FROM nmezhevova.ods_"""+i+" where EXTRACT(year FROM  "+col_date[0]+"""::DATE)={{ execution_date.year }};
    """
    )
    fill_ods = PostgresOperator(
        task_id="fill_ods_"+i,
        dag=dag,        
        sql="INSERT INTO nmezhevova.ods_"+i+" SELECT "+j+" FROM "+i+".user where EXTRACT(year FROM  "+col_date[1]+"::DATE)={{ execution_date.year }};" if i=='mdm' else \
	    "INSERT INTO nmezhevova.ods_"+i+" SELECT "+j+" FROM nmezhevova.stg_"+i+" where EXTRACT(year FROM  "+col_date[1]+"::DATE)={{ execution_date.year }};" 
         ) 
 
    clear_ods>>all_ods_clear
    all_ods_clear>>fill_ods
    fill_ods>>all_ods_loaded

view_dict={'payment':["""user_id,pay_doc_type,pay_doc_num,account,phone,billing_period,	pay_date,sum,user_id::text as user_key,	account::text as account_key,
                         billing_period::text as billing_period_key,pay_doc_type::text as pay_doc_type_key,'payment - data lake'::text as record_source""",
		      """user_id,pay_doc_type,pay_doc_num,account,phone,billing_period,	pay_date,sum,user_key,account_key,billing_period_key,
			pay_doc_type_key,record_source,
			cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as text) as user_pk,
			cast((md5(nullif(upper(trim(cast(account as varchar))), ''))) as text) as account_pk,
			cast((md5(nullif(upper(trim(cast(billing_period as varchar))), ''))) as text) as billing_period_pk,
			cast((md5(nullif(upper(trim(cast(pay_doc_type as varchar))), ''))) as text) as pay_doc_type_pk,			
			cast(md5(nullif(concat_ws('||',
				coalesce (nullif(upper(trim(cast(user_id as varchar))),''),'^^'),				
				coalesce (nullif(upper(trim(cast(billing_period as varchar))),''),'^^'),
				coalesce (nullif(upper(trim(cast(pay_doc_type as varchar))),''),'^^')),				
				'^^||^^||^^')) as text) as pay_pk,
			cast(md5(nullif(concat_ws('||',
				coalesce (nullif(upper(trim(cast(user_id as varchar))),''),'^^'),	
				coalesce (nullif(upper(trim(cast(account as varchar))),''),'^^')),
				'^^||^^')) as text) as user_account_pk,				
				cast(md5(nullif(concat_ws('||',
					coalesce (nullif(upper(trim(cast(user_id as varchar))),''),'^^'),
					coalesce (nullif(upper(trim(cast(phone as varchar))),''),'^^')),
					'^^||^^')) as text) as user_hashdiff,
				cast(md5(nullif(concat_ws('||',
				coalesce (nullif(upper(trim(cast(pay_doc_type as varchar))),''),'^^'),	
				coalesce (nullif(upper(trim(cast(pay_doc_num as varchar))),''),'^^'),
				coalesce (nullif(upper(trim(cast(sum as varchar))),''),'^^')),
				'^^||^^||^^')) as text) as pay_hashdiff""",
		      """user_id,pay_doc_type,pay_doc_num,account,phone,billing_period,	pay_date,sum,user_key,account_key,billing_period_key,
			pay_doc_type_key,record_source,	user_pk,account_pk,billing_period_pk,pay_doc_type_pk,pay_pk,user_account_pk,user_hashdiff,pay_hashdiff""",
		      ",pay_date as effective_from"],
	   'mdm':["id,legal_type,district,registered_at,billing_mode,is_vip,id::text as user_key,'mdm'::text as record_source",
		  """id,legal_type,district,registered_at,billing_mode,	is_vip,	user_key,record_source,			
			cast((md5(nullif(upper(trim(cast(id as varchar))), ''))) as text) as user_pk,						
			cast(md5(nullif(concat_ws('||',
					coalesce (nullif(upper(trim(cast(id as varchar))),''),'^^'),
					coalesce (nullif(upper(trim(cast(legal_type as varchar))),''),'^^'),
					coalesce (nullif(upper(trim(cast(district as varchar))),''),'^^'),
					coalesce (nullif(upper(trim(cast(registered_at as varchar))),''),'^^'),
					coalesce (nullif(upper(trim(cast(billing_mode as varchar))),''),'^^'),
					coalesce (nullif(upper(trim(cast(is_vip as varchar))),''),'^^')),
					'^^||^^||^^||^^||^^||^^')) as text) as mdm_hashdiff""",
		  "id,legal_type,district,registered_at,billing_mode,is_vip,user_key,record_source,user_pk,mdm_hashdiff",""],
	   'billing':["""user_id,billing_period,service,tariff,	sum,created_at,	user_id::text as user_key,billing_period::text as billing_period_key,
			service::text as service_key,tariff::text as tariff_key,'billing - data lake'::text as record_source""",
		      """user_id,billing_period,service,tariff,	sum,created_at,	user_key,billing_period_key,service_key,tariff_key,record_source,			
			cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as text) as user_pk,
			cast((md5(nullif(upper(trim(cast(billing_period as varchar))), ''))) as text) as billing_period_pk,
			cast((md5(nullif(upper(trim(cast(service as varchar))), ''))) as text) as service_pk,
			cast((md5(nullif(upper(trim(cast(tariff as varchar))), ''))) as text) as tariff_pk,			
			cast(md5(nullif(concat_ws('||',
				coalesce (nullif(upper(trim(cast(user_id as varchar))),''),'^^'),				
				coalesce (nullif(upper(trim(cast(billing_period as varchar))),''),'^^'),
				coalesce (nullif(upper(trim(cast(service as varchar))),''),'^^'),
				coalesce (nullif(upper(trim(cast(tariff as varchar))),''),'^^')),				
				'^^||^^||^^||^^')) as text) as user_billing_period_service_tariff_pk""",
		      """user_id,billing_period,service,tariff,	sum,created_at,	user_key,billing_period_key,service_key,tariff_key,record_source,
			user_pk,billing_period_pk,service_pk,tariff_pk,	user_billing_period_service_tariff_pk""",
		      ",created_at as effective_from"],
	   'issue':["""user_id,	start_time,end_time,service,description,title,user_id::text as user_key,service::text as service_key,					
			'issue - data lake'::text as record_source""",
		    """user_id,	start_time,end_time,service,description,title,user_key,	service_key,record_source,			
			cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as text) as user_pk,
			cast((md5(nullif(upper(trim(cast(service as varchar))), ''))) as text) as service_pk,						
			cast(md5(nullif(concat_ws('||',
				coalesce (nullif(upper(trim(cast(user_id as varchar))),''),'^^'),	
				coalesce (nullif(upper(trim(cast(service as varchar))),''),'^^')),				
				'^^||^^')) as text) as user_service_pk,					
				cast(md5(nullif(concat_ws('||',
				coalesce (nullif(upper(trim(cast(user_id as varchar))),''),'^^'),	
				coalesce (nullif(upper(trim(cast(service as varchar))),''),'^^'),
				coalesce (nullif(upper(trim(cast(description as varchar))),''),'^^'),
				coalesce (nullif(upper(trim(cast(title as varchar))),''),'^^')),
				'^^||^^||^^||^^')) as text) as service_hashdiff""",
		    "user_id,	start_time,end_time,service,description,title,	user_key,service_key,record_source,user_pk,service_pk,user_service_pk,service_hashdiff",""],
	   'traffic':["""user_id,event,	device_id,device_ip_addr,bytes_sent,bytes_received,user_id::text as user_key,device_id::text as device_id_key,					
			'traffic - data lake'::text as record_source""",
		      """user_id,event,	device_id,device_ip_addr,bytes_sent,bytes_received,user_key,device_id_key,record_source,			
			cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as text) as user_pk,
			cast((md5(nullif(upper(trim(cast(device_id as varchar))), ''))) as text) as device_id_pk,						
			cast(md5(nullif(concat_ws('||',
				coalesce (nullif(upper(trim(cast(user_id as varchar))),''),'^^'),		
				coalesce (nullif(upper(trim(cast(device_id as varchar))),''),'^^')),				
				'^^||^^')) as text) as user_device_pk,				
				cast(md5(nullif(concat_ws('||',
				coalesce (nullif(upper(trim(cast(user_id as varchar))),''),'^^'),	
				coalesce (nullif(upper(trim(cast(device_id as varchar))),''),'^^'),
				coalesce (nullif(upper(trim(cast(device_ip_addr as varchar))),''),'^^')),
				'^^||^^||^^')) as text) as device_hashdiff""",
		      "user_id,event,device_id,device_ip_addr,bytes_sent,bytes_received,user_key,device_id_key,record_source,user_pk,device_id_pk,user_device_pk,device_hashdiff",
		      ",event as effective_from"]}
		  
for i,j in view_dict.items():
    if i=='payment':
        col_date='pay_date'
    elif i=='billing':
        col_date='created_at'
    elif i=='issue':
        col_date='start_time'
    elif i=='traffic':
        col_date='event'
    elif i=='mdm':
        col_date='registered_at'
    view_one_year = PostgresOperator(
    task_id="view_one_year_"+i,
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        create or replace view rtk_de.nmezhevova.ods_v_"""+i+"""_{{ execution_date.year }} as (
	with staging as (
		with derived_columns as (
			select """+j[0]+" from rtk_de.nmezhevova.ods_"+i+"  where EXTRACT(year FROM  "+col_date+""")={{ execution_date.year }}),
	        hashed_columns as (
			select """+j[1]+""" from derived_columns),
		columns_to_select as (		
			select """+j[2]+""" from hashed_columns)
		select * from columns_to_select)
	select *, current_timestamp as load_date"""+j[3]+" from staging);"
    )
    all_ods_loaded >> view_one_year
    view_one_year >> all_view_create

# список сущностей для хабов
hub_lst={'payment':['user','account','pay_doc_type','billing_period'],'billing':['service','tariff'],'traffic':['device_id']}
for j in hub_lst:
    if j=='payment':
        col_date='pay_date'
    elif j=='billing':
        col_date='created_at'
    elif j=='traffic':
        col_date='event'
    for i in hub_lst[j]:
        if i=='user':
            tabl="""(select p.user_pk,p.user_key,p.load_date,p.record_source,p.pay_date from rtk_de.nmezhevova.ods_v_payment_{{ execution_date.year }} as p
	    	union select b.user_pk,b.user_key,b.load_date,b.record_source,b.created_at as pay_date from rtk_de.nmezhevova.ods_v_billing_{{ execution_date.year }} as b
		union select is.user_pk,is.user_key,is.load_date,is.record_source,is.start_time as pay_date from rtk_de.nmezhevova.ods_v_issue_{{ execution_date.year }} as is
		union select t.user_pk,t.user_key,t.load_date,t.record_source,t.event as pay_date from rtk_de.nmezhevova.ods_v_traffic_{{ execution_date.year }} as t
                union select m.user_pk,m.user_key,m.load_date,m.record_source,m.registered_at as pay_date from rtk_de.nmezhevova.ods_v_mdm_{{ execution_date.year }} as m )
		as u"""
        elif i=='billing_period':
            tabl="""(select p.billing_period_pk,p.billing_period_key,p.load_date,p.record_source,p.pay_date from rtk_de.nmezhevova.ods_v_payment_{{ execution_date.year }} as p
	    	union select b.billing_period_pk,b.billing_period_key,b.load_date,b.record_source,b.created_at as pay_date from rtk_de.nmezhevova.ods_v_billing_{{ execution_date.year }} as b)
		as bil"""
        elif i=='service':
            tabl="""(select b.service_pk,b.service_key,b.load_date,b.record_source,b.created_at as pay_date from rtk_de.nmezhevova.ods_v_billing_{{ execution_date.year }} as b
	    	union select is.service_pk,is.service_key,is.load_date,is.record_source,is.start_time as pay_date from rtk_de.nmezhevova.ods_v_issue_{{ execution_date.year }} as is)
		as ser"""
        else:
            tabl="rtk_de.nmezhevova.ods_v_"+j+"_{{ execution_date.year }}"
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
			        from """+tabl+" where EXTRACT(year FROM  "+col_date+""")={{ execution_date.year }}) as h
		        where row_number = 1)
            insert into rtk_de.nmezhevova.dds_hub_"""+i+\
	            " select a."+i+"_pk,a."+i+"""_key,a.load_date,a.record_source
	            from row_rank_1 as a
	            left join rtk_de.nmezhevova.dds_hub_"""+i+""" as d
	            on a."""+i+"_pk=d."+i+"""_pk
	            where d."""+i+"""_pk is null;
        	"""
        )
        all_view_create>>dds_hub
        dds_hub >> all_hubs_loaded

 
# словарь соответсвия названия линков и набора колонок для вставок, ключей    
link_dict={'payment':["pay_pk,user_pk, billing_period_pk, pay_doc_type_pk, effective_from, load_date,sum, record_source",		      
                      "a.pay_pk,a.user_pk, a.billing_period_pk, a.pay_doc_type_pk, a.effective_from, a.load_date,a.sum, a.record_source",
                      "pay"],
           'user_account':["user_account_pk,user_pk, account_pk, load_date, record_source",
                           "a.user_account_pk,a.user_pk, a.account_pk, a.load_date, a.record_source",
                          "user_account"],
	  'user_billing_period_service_tariff':["user_billing_period_service_tariff_pk,user_pk, billing_period_pk,service_pk,tariff_pk, effective_from, load_date,sum, record_source",
						"a.user_billing_period_service_tariff_pk,a.user_pk, a.billing_period_pk,a.service_pk,a.tariff_pk, a.effective_from, a.load_date,a.sum, a.record_source",
					       "user_billing_period_service_tariff"],
	  'user_service':["user_service_pk,user_pk, service_pk,start_time, end_time, load_date,  record_source",
			  "a.user_service_pk,a.user_pk, a.service_pk,a.start_time, a.end_time, a.load_date,  a.record_source",
			  "user_service"],
	  'user_device':["user_device_pk,user_pk, device_id_pk,effective_from, bytes_sent,bytes_received, load_date, record_source",
			 "a.user_device_pk,a.user_pk, a.device_id_pk,a.effective_from, a.bytes_sent,a.bytes_received, a.load_date, a.record_source",
			 "user_device"]}

for i,j in link_dict.items():
    if i=='payment' or i=='user_account':
        ods_tabl='payment'
        col_date1='pay_date'
    elif i=='user_billing_period_service_tariff':
        ods_tabl='billing'
        col_date1='created_at'
    elif i== 'user_service':
        ods_tabl='issue'
        col_date1='start_time'
    elif i== 'user_device':
        ods_tabl='traffic'
        col_date1='event'
    dds_link = PostgresOperator(
    task_id="dds_link_"+i,
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
           with row_rank_1 as (
             select distinct """+j[0]+\
	                    " from rtk_de.nmezhevova.ods_v_"+ ods_tabl+
	                "_{{ execution_date.year }} where EXTRACT(year FROM  "+col_date1+""")={{ execution_date.year }})
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
           'pay':["a.user_pk,a.pay_doc_type_pk,a.pay_hashdiff,a.pay_doc_num, a.effective_from,a.load_date,a.record_source",
                  "c.user_pk,c.pay_doc_type_pk,c.pay_hashdiff,c.load_date",
                  "e.user_pk,e.pay_doc_type_pk,e.pay_hashdiff,e.pay_doc_num,e.effective_from,e.load_date,e.record_source"],
	 'service':["a.user_service_pk,a.user_pk,a.service_pk,a.service_hashdiff,a.description,a.title,a.start_time,a.end_time,a.load_date,a.record_source",
		    "c.user_service_pk,c.user_pk,c.service_pk,c.service_hashdiff,c.load_date",
		    "e.user_service_pk,e.user_pk,e.service_pk,e.service_hashdiff,e.description,e.title,e.start_time,e.end_time,e.load_date,e.record_source"],
	 'device':["a.device_id_pk,a.device_hashdiff,a.device_ip_addr,a.effective_from,a.load_date,a.record_source",
		   "c.device_id_pk,c.device_hashdiff,c.load_date",
		   "e.device_id_pk,e.device_hashdiff,e.device_ip_addr,e.effective_from,e.load_date,e.record_source"],
	 'mdm':["a.user_pk,a.mdm_hashdiff,a.legal_type,a.district,a.registered_at,a.billing_mode,a.is_vip,a.load_date,a.record_source",
		"c.user_pk,c.mdm_hashdiff,c.load_date",
		"e.user_pk,e.mdm_hashdiff,e.legal_type,e.district,e.registered_at,e.billing_mode,e.is_vip,e.load_date,e.record_source"]}

for i,j in sat_dict.items():
    if i=='user' or i=='pay':
        ods_tabl1='payment'
        ods_key='user_pk'
        col_date2='pay_date'
    elif i=='service':
        ods_tabl1='issue'
        ods_key="user_service_pk"
        col_date2='start_time'
    elif i== 'device':
        ods_tabl1='traffic'
        ods_key='device_id_pk'
        col_date2='event'
    elif i== 'mdm':
        ods_tabl1='mdm'
        ods_key='user_pk'
        col_date2='registered_at'
    dds_sat = PostgresOperator(
    task_id="dds_sat_"+i+"_details",
    dag=dag,
        # postgres_conn_id="postgres_default",
    sql="""
                with source_data as (
		    select """+j[0]+\
		    " from rtk_de.nmezhevova.ods_v_"+ods_tabl1+"""_{{ execution_date.year }} as a
                   where EXTRACT(year FROM  """+col_date2+""")={{ execution_date.year }}),
		
		update_records as (
		select """+j[0]+\
		" from rtk_de.nmezhevova.dds_sat_"+i+"""_details as a
		join source_data as b
		on a."""+ods_key+"=b."+ods_key+"""),
		
		latest_records as (
			select * from (
				select """+j[1]+""",
				case when rank() over (partition by c."""+ods_key+""" order by c.load_date desc)=1
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
	    
sources1=['payment','billing','issue','traffic','mdm']
for i in sources1:
    drop_view = PostgresOperator(
    task_id="drop_view_one_year_"+i,
    dag=dag,
        # postgres_conn_id="postgres_default",
    sql="drop  view if exists rtk_de.nmezhevova.ods_v_"+i+"_{{ execution_date.year }};"
    )
    all_sat_loaded >>drop_view
    drop_view>>all_view_drop


