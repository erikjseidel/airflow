from __future__ import annotations

import logging, shutil, sys, tempfile, time, pendulum
from airflow import DAG, AirflowException
from airflow.models.param import Param
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator
from includes.salt_api import SaltAPI, salt_api_task

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

BASE_DIR = tempfile.gettempdir()

with DAG(
    dag_id="create_pni",
    schedule=None,
    start_date=pendulum.datetime(1970, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pni"],
    params={
        'salt_master': Param(
            "netops2",
            type="string",
            title="Salt Master",
            description="Select salt master to execute salt commands",
            enum=["netops2"],
        ),
        'device': Param(
            "red_herring", 
            title="Device",
            description="New PNI target device",
            type="string"
            ),
        'interface': Param(
            "red_herring", 
            title="Interface",
            description="New PNI target interface",
            type="string"
            ),
        'peer_name': Param(
            "red_herring", 
            title="Peer Name",
            description="Name of new PNI peer",
            type="string"
            ),
        'circuit_id': Param(
            "red_herring", 
            title="Circuit ID",
            description="PNI Circuit ID",
            type="string"
            ),
        },
) as dag:

    @task(task_id="netbox_create_circuit")
    @salt_api_task
    def netbox_create_circuit(ds=None, **context):

        params = context['params']

        data =  {
                'device'    : params['device'],
                'interface' : params['interface'],
                'peer_name' : params['peer_name'],
                'cid'       : params['circuit_id'],
                'test'      : False,
                }

        return SaltAPI(params['salt_master']).runner_client(fun='netbox.create_pni', saltkwargs=data)

    run_netbox_create_circuit = netbox_create_circuit()


    @task(task_id="salt_sync_interfaces")
    @salt_api_task
    def salt_sync_interfaces(ds=None, **context):

        params = context['params']

        return SaltAPI(params['salt_master']).runner_client(fun='netbox.synchronize_interfaces', saltkwargs={'test': False})

    run_salt_sync_interfaces = salt_sync_interfaces()


    @task(task_id="salt_apply_pni")
    @salt_api_task
    def salt_apply_pni(ds=None, **context):

        params = context['params']

        return SaltAPI(params['salt_master']).local_client(params['device'], 'ethernet.apply_pni', [ params['interface'] ])

    run_salt_apply_pni = salt_apply_pni()

    run_netbox_create_circuit >> run_salt_sync_interfaces >> run_salt_apply_pni
