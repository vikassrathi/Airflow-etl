from pyspark.sql import functions as f
from pyspark.sql import SparkSession

from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("Airflowlearn").getOrCreate()

default_args = {
    'owner': 'Illion',
    'retries': 1,
    'retry_delay': timedelta(seconds=50)
}

def read_csv_file(**kwargs):
    df = spark.read.csv('gs://temp_table_learn/insurance.csv')
    return df

def remove_null_values(ti, **kwargs):
    df = ti.xcom_pull(task_ids='read_csv_file')
    df = df.na.drop('all')
    return df

def group_by_smoker(ti, **kwargs):
    df = ti.xcom_pull(task_ids='remove_null_values')
    smoker_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    })
    smoker_df.write.format('csv').mode('overwrite').save('gs://temp_table_learn/output/groupbysmoke.csv')

def groupby_region(ti, **kwargs):
    df = ti.xcom_pull(task_ids='remove_null_values')
    region_df = df.groupby('region').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    })
    region_df.write.format('csv').mode('overwrite').save('gs://temp_table_learn/output/groupbyregion.csv')

with DAG(
    dag_id='learn_airflow',
    description='Running an airflow pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['python', 'transform', 'pipeline']
) as dag:

    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values,
        provide_context=True
    )

    group_by_smoker = PythonOperator(
        task_id='group_by_smoker',
        python_callable=group_by_smoker,
        provide_context=True
    )

    groupby_region = PythonOperator(
        task_id='groupby_region',
        python_callable=groupby_region,
        provide_context=True
    )

    read_csv_file >> remove_null_values >> [group_by_smoker, groupby_region]
