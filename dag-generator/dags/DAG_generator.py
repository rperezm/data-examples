import json
import datetime
from airflow.models import Variable
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcp_container_operator import GKEPodOperator
from google.cloud import storage

def create_dag(conf_dag):
    dag_id = conf_dag['name']
    project = conf_dag['project']
    enviroment = conf_dag['enviroment']
    origin_system = conf_dag['origin_system']
    landing_bucket = conf_dag['landing_bucket']
    raw_bucket = conf_dag['raw_bucket']
    business_unit = conf_dag['business_unit']
    bq_raw_project = conf_dag['bq_raw_project']
    bq_raw_dataset = conf_dag['bq_raw_dataset']
    country_id = conf_dag['country_id']

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

    # Date from Airflow Rest API
    process_date = """{{ macros.ds_format(dag_run.conf['process_date'],'%Y-%m-%d','%Y%m%d') }}"""
    process_date_dash = "{{ dag_run.conf['process_date'] }}"
    epoch = "{{ dag_run.conf['epoch_date'] }}"

    default_args = {
        'owner': 'example - reperezm',
        'depends_on_past': False,
        'start_date': yesterday,
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=1)
    }

    dag = DAG(dag_id, catchup=False, default_args=default_args, schedule_interval=None)

    begin = DummyOperator(
        task_id='Begin',
        dag=dag)

    end = DummyOperator(
        task_id='End',
        dag=dag)

    # Generate Dummies operator if Necessary
    if conf_dag["generate_dummy_operator"] != "None":
        for dummy_operator in conf_dag["generate_dummy_operator"]:
            vars()[dummy_operator] = DummyOperator(
                task_id=dummy_operator,
                dag=dag)
                
    for raw_table in conf_dag['raw_tables']:

        result_file_name = f"raw_{business_unit}_{country_id}_{enviroment}_{origin_system}_{raw_table}"

        job_id = f'{country_id}_{raw_table}_{process_date}_{epoch}'

        task_l2r = GKEPodOperator(
            task_id=f'l2r_{raw_table}',
            project_id=project,
            location='my-location',
            cluster_name='my_cluster-name',
            name='k8s-movefile-l2r',
            namespace='my-namespace',
            image='us.gcr.io/my-image',
            cmds=['python', 'move_file.py'],
            arguments=[landing_bucket, raw_bucket, raw_table, job_id, conf_dag['extension_file'],result_file_name, process_date],
            is_delete_operator_pod=True,
            dag=dag)

        # based in architecture nomenclature : raw_<Business Unit>_<location>_<enviroment>_<origin system>_<origin table>_<data_date>_<timestamp>
        file_prefix = f"raw_{business_unit}_{country_id}_{enviroment}_{origin_system}_{raw_table}_{process_date}"
        conf_partition = conf_dag['table_partitions'][raw_table]
        raw_write_disposition = "WRITE_TRUNCATE" if conf_partition == "None" else "WRITE_APPEND"

        # Clean Bigquery raw tables, if table contains partitions
        if conf_partition != 'None':
            task_clean_table = BigQueryOperator(
                task_id=f'cln_{raw_table}',
                sql=f"DELETE FROM `{bq_raw_project}.{bq_raw_dataset}.{raw_table}` WHERE {conf_partition}".replace(
                    '@my-value-column', process_date_dash),
                use_legacy_sql=False,
                allow_large_results=True,
                bigquery_conn_id='bigquery_default',
                dag=dag)

        # Load to Bigquery from specific file
        task_l2q = GKEPodOperator(
            task_id=f'load_{raw_table}',
            project_id=project,
            location='my-location',
            cluster_name='com-prd-sod-cl-k8s',
            name='k8s-rawfile-to-bigquery',
            namespace='my-namespace',
            image='us.gcr.io/my-image',
            cmds=['python', 'load_bigquery.py'],
            arguments=[bq_raw_project, bq_raw_dataset, raw_table, raw_write_disposition,
                       raw_bucket, f'{raw_table}/{file_prefix}', conf_dag['extension_file']],
            is_delete_operator_pod=True,
            dag=dag)

        if conf_partition != 'None':
            begin >> task_l2r >> task_clean_table >> task_l2q >> end
        else:
            begin >> task_l2r >> task_l2q >> end

    # Orquestation from json config file
    for item in conf_dag['orquestation']:
        eval(item)

    return dag

####### Generic Dag Generator from json file #######
# Read bucket configuration files : data/conf/my_config_file
client = storage.Client()

composer_bucket = Variable.get('composer_bucket')
bucket = client.get_bucket(composer_bucket)

# my configs file
blobs = client.list_blobs(composer_bucket, prefix='data/conf')

for blob in blobs:
    blob_name = blob.name.split('/')[3]
    if blob_name != '':
        blob_dag = bucket.get_blob(blob.name)
        json_conf_dag = blob_dag.download_as_string()
        conf_dag = json.loads(json_conf_dag)
        dag_id = conf_dag['name']
        globals()[dag_id] = create_dag(conf_dag)