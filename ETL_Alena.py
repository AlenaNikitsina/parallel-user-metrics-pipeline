from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#коннект к базе данных

def ch_get_df(query='Select 1', host='http://clickhouse.lab.karpov.courses:8123', user='student', password='dpo_python_2020'):
    connection = {
        'host': 'http://clickhouse.lab.karpov.courses:8123',
        'database':'simulator_20251220',
        'user':'student',
        'password':'...'
}
    result = ph.read_clickhouse(query, connection=connection)
    return result


# Дефолтные параметры
default_args = {
    'owner': 'elena-prihodko',
    'depends_on_past': False,
    'retries': 2, 
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 19),
}

# Интервал запуска DAG
schedule_interval = '12 12 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_sim_prihodko():
    
    #извлекаем данные из таблицы: В feed_actions для каждого юзера посчитаем число просмотров и лайков контента.
    @task()
    def extract_feed_actions():
        query_feed = """Select
        Date(time) as event_date,
        user_id,
        countIf(action = 'view') as views,
        countIf(action = 'like') as likes,
        gender,
        age,
        os
        From
        simulator_20251220.feed_actions
        Where
        Date(time) = yesterday()
        Group by
        event_date,
        user_id,
        gender,
        age,os
        """
        df_feed_actions=ch_get_df(query=query_feed)
        return df_feed_actions
    
    #извлекаем данные из таблицы:В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. 
    @task()
    def extract_message_actions():
        q_message = """
        WITH user_received as(
        Select
        Date(time) as event_date,
        receiver_id as user_id,
        count(receiver_id) as messages_received,
        uniqExact(user_id) as users_received,
        gender,
        age,os
        From
        simulator_20251220.message_actions
        where
        toDate(time) = yesterday()
        Group by
        event_date,
        receiver_id,
        gender,
        age,
        os
        ),
        user_sent as(
        Select
        Date(time) as event_date,
        user_id,
        count(receiver_id) as messages_sent,
        uniqExact(receiver_id) as users_sent
        From
        simulator_20251220.message_actions
        Where
        toDate(time) = yesterday()
        Group by
        event_date,
        user_id
        )
        Select
        event_date,user_id,messages_received,messages_sent,users_received,users_sent,gender,age,os
        From
        user_received
        OUTER JOIN user_sent using (user_id)
        Order by
        user_id"""
        df_message_actions=ch_get_df(query=q_message)
        return df_message_actions
   
    #объединяем две таблицы в одну
    @task()
    def transfrom_df_merge(df_feed_actions, df_message_actions):
        df_merge=pd.merge(df_feed_actions, df_message_actions,
                           on=['user_id', 'event_date', 'gender', 'age', 'os'],
                          how='outer'
                         ).fillna(0)
        return df_merge
    
    #Считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез
    @task()
    def transfrom_gender(df_merge):
        df_gender=df_merge.groupby(['event_date','gender'])['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum().reset_index()
        df_gender = df_gender.rename(columns={'gender': 'dimension_value'})
        df_gender.insert(1, 'dimension', 'gender')
        return df_gender
    
    @task()
    def transfrom_age(df_merge):
        df_age=df_merge.groupby(['event_date','age'])['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum().reset_index()
        df_age = df_age.rename(columns={'age': 'dimension_value'})
        df_age.insert(1, 'dimension', 'age')
        return df_age
    
    @task()
    def transfrom_os(df_merge):
        df_os=df_merge.groupby(['event_date','os'])['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum().reset_index()
        df_os = df_os.rename(columns={'os': 'dimension_value'})
        df_os.insert(1, 'dimension', 'os')
        return df_os
    
    @task()
    def transfrom_union_tables(df_gender, df_os, df_age):
        df_data = pd.concat([df_gender, df_os, df_age], ignore_index=True)
        columns_int = ['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']
        df_data[columns_int] = df_data[columns_int].astype(int)
        return df_data
    
    # Финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse
    @task()
    def load(df_data):
        connection_test = {'host': 'http://clickhouse.lab.karpov.courses:8123',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'....'
                     }
        #запрос создания таблички 
        query_test = '''
        CREATE TABLE IF NOT EXISTS test.elena_prihodko(
        event_date Date,
        dimension String,
        dimension_value String, 
        views Int64,
        likes Int64,
        messages_received Int64,
        messages_sent Int64,
        users_received Int64, 
        users_sent Int64
        )
        ENGINE = MergeTree()
        ORDER BY (event_date, dimension, dimension_value)
        '''
        ph.execute(query_test, connection=connection_test)
        
        #Функция,чтобы каждый день таблица дополнялась новыми данными
        ph.to_clickhouse(df=df_data, table="elena_prihodko", 
                 index=False, connection=connection_test)
        
    df_feed_actions = extract_feed_actions()
    df_message_actions = extract_message_actions()
    
    df_merge = transfrom_df_merge(df_feed_actions, df_message_actions)
    df_gender = transfrom_gender(df_merge)
    df_age = transfrom_age(df_merge)
    df_os = transfrom_os(df_merge)
    df_data = transfrom_union_tables(df_gender, df_os, df_age)
    
    load(df_data)

dag_sim_prihodko = dag_sim_prihodko()
        
        

