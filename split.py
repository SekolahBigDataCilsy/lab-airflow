from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime as dt
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 5, 10, 10, 00, 00),
    'retries': 1,
    'concurrency': 5

}

def transform_csv():
    # get data csv file
    data = pd.read_csv('https://media.geeksforgeeks.org/wp-content/uploads/nba.csv')
    data.dropna(inplace=True)

    # tranformation split column and convert data type to int64 
    # split firstname and lastname
    new = data["Name"].str.split(" ", n = 1, expand = True)
    data["Firstname"] = new[0]
    data["Lastname"] = new[1]
    data.drop(columns =["Name"], inplace = True)

    # convert data type column age become int64
    data['Age'] = data['Age'].astype('int64')

    # convert data type column number become int64
    data['Number'] = data['Number'].astype('int64')

    # tampilkan data setelah transformasi
    print(data.head(10))

    # save to csv
    data.to_csv('/usr/data/csv/data_after_transform.csv')
    print('Data berhasil di save ke csv')


def transform_json():
    # get data csv file
    data_pns = pd.read_csv('https://data.jakarta.go.id/dataset/784b2bba-b74f-45a3-a7dd-ca58fe0a0c11/resource/162cd06d-0970-446a-9705-dc8cc04c2ac7/download/revisi-datapegawaipnsguru-2019.csv')
    data_pns.dropna(inplace=True)

    # tampilkan data
    print(data_pns.head(10))

    # save to json
    data_pns.to_json('/usr/data/json/data_pns.json')
    print('Data berhasil di save ke json')

def welcome():
    print('Welcome to Apache Airflow')
    print('First run this task then execute task transform_csv and transform_json parallel')
    print('Then finally run task end_task')

def end_task():
    print('This task is executed after run task welcome, transform_csv and transform_json')
    print('End')

with DAG('transform_file', default_args=default_args, schedule_interval='@once', catchup=False) as dag:
    task_welcome = PythonOperator(task_id='say_welcome', python_callable=welcome)

    task_transform_csv = PythonOperator(task_id='trans_csv', python_callable=transform_csv)

    task_transform_json = PythonOperator(task_id='trans_json', python_callable=transform_json)

    task_end = PythonOperator(task_id='say_end', python_callable=end_task)

task_welcome >> task_transform_csv >> task_end
task_welcome >> task_transform_json >> task_end