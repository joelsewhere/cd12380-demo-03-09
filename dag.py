from airflow.sdk import dag, task
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


BUCKET="l3-external-storage-753900908173"
S3_KEY = '{{ dag.dag_id }}/extract/{{ ds }}/author_page_views.csv',


@dag
def author_metrics():

    @task
    def extract(extract_key):
        
        import pathlib
        import pandas as pd
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        
        csv_path = pathlib.Path(__file__).parent / 'author_page_views.csv'
        df = pd.read_csv(csv_path)

        hook = S3Hook()

        hook.load_string(
            string_data=df.to_csv(index=False),
            bucket=BUCKET,
            key=extract_key
        )

    load = S3ToRedshiftOperator(
        task_id="quotes",
        table="author_page_views",
        schema="scraped_quotes",
        s3_bucket=BUCKET,
        s3_key=S3_KEY,
        method='UPSERT',
        upsert_keys=['page_view_id'],
        copy_options=[
            "CSV",
            "IGNOREHEADER 1",
            ],
        )
    
    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="redshift_default",
        sql="CREATE SCHEMA IS NOT EXISTS analytics.scraped_quotes;"
    )
    
    transform = SQLExecuteQueryOperator(
        task_id="transform",
        conn_id='redshift_default',
        sql="sql/author_metrics.sql",
        params={
            "database": "analytics",
            "schema": "scraped_quotes",
            "table": "author_metrics"
        }
    )

    extract() >> load >> create_schema >> transform


author_metrics()
    
    
    




