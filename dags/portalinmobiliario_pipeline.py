from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.models.baseoperator import chain
from datetime import datetime

@dag(
    start_date = datetime(2023, 8, 31),
    schedule = "@monthly",
    catchup = True,
    tags = ['portalinmobiliario'],
)
def portalinmobiliario():
    # TODO: Task 0 - Schedule scrape
    # This task may not be easy, the scrape script uses selenium
    # so it needs a WebDriver. Maybe running it inside a Docker container
    # in some way. Research required.
    # Ideal scheduling: once per week, and trigger re-run of the whole pipeline.

    # TASK: Execute UF value scraper
    @task.external_python(python = '/Users/facualex/Documents/development/data-engineering/deptos-RM/deptosrm-env/bin/python')
    def scrape_uf_values():
        from common.scraper.ufvalues import update_uf_csv
        from datetime import datetime

        return update_uf_csv(csv_file_path="/Users/facualex/Documents/development/data-engineering/deptos-RM/data/uf-values.csv",
                             new_data_date=datetime.now().strftime('%d-%m-%Y'))
    
    # TASK: Upload UF csv dataset to GCS Bucket 
    upload_uf_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_uf_csv_to_gcs',
        src = './data/uf-values.csv', # csv path we want to upload
        dst = 'raw/uf-values.csv', # destination in GCS
        bucket = 'facundoalexandre_portalinmobiliario',
        gcp_conn_id = 'gcp', # airflow connection to gcp name
        mime_type = 'text/csv'
    ) 
   
    # TASK 1: Upload apartments dataset to GCS Bucket
    upload_apartments_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_apartments_csv_to_gcs',
        src = './data/apartments.csv', # csv path we want to upload
        dst = 'raw/apartments.csv', # destination in GCS
        bucket = 'facundoalexandre_portalinmobiliario',
        gcp_conn_id = 'gcp', # airflow connection to gcp name
        mime_type = 'text/csv'
    ) 

    # TASK 2: Create empty dataset on BigQuery to prepare our data transfer
    create_empty_biqguery_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id = "create_empty_bigquery_dataset",
        dataset_id = "portalinmobiliario",
        gcp_conn_id = 'gcp',
    )

    # TASK: Load UF dataset from GCS to a BigQuery Table
    uf_dataset_from_gcs_to_bq = GCSToBigQueryOperator(
        task_id='uf_dataset_from_gcs_to_bq',
        bucket='facundoalexandre_portalinmobiliario',
        source_objects=['raw/uf-values.csv'],
        destination_project_dataset_table='portalinmobiliario.uf-values',
        schema_fields=[
            {'name': 'date', 'type': 'STRING'},
            {'name': 'value', 'type': 'STRING'},
        ],
        write_disposition='WRITE_APPEND',  # Choose from WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        gcp_conn_id = 'gcp',
    )

    # TASK 3: Load apartments dataset from GCS to a BigQuery Table
    apartments_dataset_from_gcs_to_bq = GCSToBigQueryOperator(
        task_id='apartments_dataset_from_gcs_to_bq',
        bucket='facundoalexandre_portalinmobiliario',
        source_objects=['raw/apartments.csv'],
        destination_project_dataset_table='portalinmobiliario.apartments-rm',
        schema_fields=[
            {'name': 'direccion', 'type': 'STRING'},
            {'name': 'zona', 'type': 'STRING'},
            {'name': 'comuna', 'type': 'STRING'},
            {'name': 'precio', 'type': 'INTEGER'},
            {'name': 'dormitorios', 'type': 'INTEGER'},
            {'name': 'superficie', 'type': 'INTEGER'}
        ],
        write_disposition='WRITE_APPEND',  # Choose from WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        gcp_conn_id = 'gcp',
    )

    # TASK 4: Run SODA Data Check for BigQuery Data Loads 
    @task.external_python(python = '/Users/facualex/Documents/development/data-engineering/deptos-RM/deptosrm-env/bin/python')
    def check_gbq_load(scan_name = 'check_gbq_load', checks_subpath = 'sources'):
        from common.soda.data_check_function import check

        return check(scan_name,
                     checks_subpath,
                     config_file="/Users/facualex/Documents/development/data-engineering/deptos-RM/include/soda/configuration.yml",
                     checks_path="/Users/facualex/Documents/development/data-engineering/deptos-RM/include/soda/checks")

    # TASK 5: dbt Transformation - Create Star Schema

    chain(scrape_uf_values(),
          upload_apartments_csv_to_gcs,
          upload_uf_csv_to_gcs,
          create_empty_biqguery_dataset,
          uf_dataset_from_gcs_to_bq,
          apartments_dataset_from_gcs_to_bq,
          check_gbq_load())

portalinmobiliario()