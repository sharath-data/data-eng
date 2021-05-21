# Use the below DAG to ingest lineage data from BQ to Datahub using an Datahub emitter task

from collections import defaultdict
from datetime import datetime
from pathlib import Path

from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery

from workflows.airflow_DAG import DAG  # noqa
from workflows import common  # noqa

import datahub.emitter.mce_builder as builder
from datahub.integrations.airflow.operators import DatahubEmitterOperator

BQ_PROJECT = 'bq_project_name'
SVC_ACCOUNT = 'xyz@project.iam.gserviceaccount.com'

BQ_LINEAGE = f'''
   with bq_lineage as
    (
        select
            distinct
            split(destinationTable, ".")[SAFE_OFFSET(1)] destination_dataset,
            split(destinationTable, ".")[SAFE_OFFSET(2)] destination_table,
            split(src, ".")[SAFE_OFFSET(1)] source_dataset, split(src, ".")[SAFE_OFFSET(2)] source_table,
        from `{BQ_PROJECT}.fnd_audit_bq.queries`, unnest(split(sourceTables, ",")) src
        where principalEmail in ('{SVC_ACCOUNT}') # service accounts used by Airflow
        and split(destinationTable, ".")[SAFE_OFFSET(0)] = '{BQ_PROJECT}'
        and destinationTable != sourceTables # for delete statements
        # use conditions blelow to filter housekeeping fields
        # and split(src, ".")[SAFE_OFFSET(1)] not like '_script%'
        # and (split(destinationTable, ".")[SAFE_OFFSET(1)] not like 'personal_%' # personal_ represent personal dataset
        # and (split(src, ".")[SAFE_OFFSET(1)] not like 'personal_%')) # personal_ represent personal dataset        
    ),
    bq_table_list as
    (
        select source_dataset, source_table, destination_dataset, destination_table
        from bq_lineage
        /************ SET YOUR DATASET HERE ************/
        where 1=1
            and destination_dataset in (select schema_name from INFORMATION_SCHEMA.SCHEMATA)
    )
    select distinct *
    from bq_table_list
    order by destination_dataset, destination_table
    # limit 10
'''

DAG_ID = Path(__file__).parent.name
DEFAULT_ARGS = {
  'owner': 'schandra',
  'start_date': datetime(2020, 12, 15),
  'on_failure_callback': common.slack_notify_for_failure
}


with DAG(DAG_ID, default_args=DEFAULT_ARGS, schedule_interval='@daily') as dag:

    def get_hierarchy(**kwargs):

        client = bigquery.Client()
        result = client.query(BQ_LINEAGE)

        raw_hierarchy = defaultdict(list)

        for row in result:
            dest = f'{row.destination_dataset}.{row.destination_table}'
            src = f'{row.source_dataset}.{row.source_table}'
            raw_hierarchy[dest].append(src)

        return raw_hierarchy


    listTables = PythonOperator(task_id='get_hierarchy', python_callable=get_hierarchy, provide_context=True)

    def print_tables(**kwargs):
        ti = kwargs['ti']
        mces = []
        raw_hierarchy = ti.xcom_pull(task_ids='get_hierarchy')
        for dest, src in raw_hierarchy.items():
            upstream_urs = [builder.make_dataset_urn("bigquery", f"{BQ_PROJECT}.{i}") for i in src]
            downstream_urn = builder.make_dataset_urn("bigquery", f"{BQ_PROJECT}.{dest}")
            mces.append(builder.make_lineage_mce(upstream_urns=upstream_urs, downstream_urn=downstream_urn))
        emit_lineage_task = DatahubEmitterOperator(
            task_id="emit_lineage",
            datahub_conn_id="datahub_sandbox_rest_default",
            mces=mces)

        listTables >> emit_lineage_task
