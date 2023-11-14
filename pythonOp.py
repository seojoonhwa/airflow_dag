from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pendulum
from kubernetes.client import models as k8s
K8S_VOLUMES=[
    k8s.V1Volume(
        name='airflow-shared-pv',
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='airflow-shared-claim'),
    )
]

K8S_VOLUME_MOUNTS=[
    k8s.V1VolumeMount(name='airflow-shared-pv', mount_path='/shared/')
]

executor_config = {
    "pod_override": k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            namespace='airflow'
        ),
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name='base',
                    volume_mounts=K8S_VOLUME_MOUNTS,
                )
            ],
            volumes=K8S_VOLUMES,
        )
    )
}

dag = DAG(
    dag_id='pythonOp',
    # test용으로 한번만 돌릴 때 @once를 사용
    schedule_interval='@once',
    start_date=datetime(2023,8,21,10,17)
    # schedule=timedelta(minutes=1)
    )


# python Operator
def createFile(location, filename):
    with open(f"{location}/{filename}", 'a') as f:
        f.write('test')

# op_kwargs는 dict, op_args는 list
a = PythonOperator(task_id="t1", python_callable=createFile, op_kwargs={'location': '/shared','filename': 'touch.txt'}, dag=dag, executor_config=executor_config)

def catFile():
    with open(f"/shared/touch.txt", 'r') as f:
        text = f.read()
    print(text)
b = PythonOperator(task_id="t2", python_callable=catFile,  dag=dag, executor_config=executor_config)

a >> b
