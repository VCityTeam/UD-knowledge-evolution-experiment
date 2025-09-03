from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script,
    Container,
    Env,
    Resources,
    Task,
    Resource,
    ExistingVolume,
    UserContainer,
)
from hera.shared import global_config
from hera.workflows.models import Toleration, Arguments, Parameter, ValueFrom, ImagePullPolicy, SecurityContext, PodSecurityContext, RetryStrategy, IntOrString
from experiment_constants import constants
from experiment_utils import create_service_manifest, create_cleanup_config
import os

@script(inputs=[Parameter(name="version"), Parameter(name="product"), Parameter(name="step"), Parameter(name="workflow_id")],
        outputs=[
            Parameter(name="jena-name",
                      value_from=ValueFrom(path="/tmp/jena-name"))
])
def compute_jena_configurations(version: str, product: str, step: str, workflow_id: str):
    with open("/tmp/jena-name", "w") as f_out:
        f_out.write(f"{workflow_id}-jena-{version}-{product}-{step}")

@script(
    image=constants.python_requests,
    inputs=[
        Parameter(name="existing_volume_name",
                  description="The name of the existing volume containing the data to import"),
        Parameter(name="number_of_versions",
                  description="The number of versions to import"),
        Parameter(name="hostname",
                  description="The hostname of the server to import the data into"),
    ],
    volumes=[
        ExistingVolume(
            name="{{inputs.parameters.existing_volume_name}}",
            claim_name="{{inputs.parameters.existing_volume_name}}",
            mount_path="/app/data",
        )
    ],
    resources=Resources(memory_limit=f"{constants.memory_limit}Gi"),
    env=[
        Env(name="PYTHONUNBUFFERED", value="1"),
        Env(name="JENA_ADMIN_PASSWORD", value=constants.postgres_password)
    ]
)
def create_theoretical_dataset_importer(
    number_of_versions: int,
    hostname: str
) -> None:
    from datetime import datetime
    import os
    import time
    import requests
    import sys

    directory = "/app/data/data/theoretical"

    try:
        print(
            f"Directory: {directory}, Number of versions: {number_of_versions}, Hostname: {hostname}")

        # Get the list of files and directories in the specified directory
        files_and_directories = os.listdir(directory)

        # Filter out directories, keeping only files
        files = [f for f in files_and_directories if os.path.isfile(
            os.path.join(directory, f))]

        for file in files:
            if file.endswith(".ttl.trig"):
                # Extraire le numéro de version à partir du nom de fichier
                version = int(file.split('-')[-1].split('.ttl')[0])

                # Vérifier si la version est inférieure ou égale au nombre de versions spécifiées
                if version <= number_of_versions:
                    print(
                        f"\n{datetime.now().isoformat()} - [Triple Store] Version {file}")
                    start = int(time.time() * 1000)
                    filepath = os.path.join(directory, file)

                    # Effectuer la requête HTTP pour importer la version
                    try:
                        with open(filepath, 'r') as f:
                            jena_admin_password = os.environ.get('JENA_ADMIN_PASSWORD', '')
                            url = f"http://admin:{jena_admin_password}@{hostname}-service:3030/mydataset/data"
                            response = requests.post(
                                url,
                                headers={'Content-Type': 'application/trig'},
                                data=f.read(),
                                timeout=18000  # 5 hours in seconds
                            )
                            response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        print(f"Failed to import {filepath}: {e}")
                        sys.exit(1)

                    end = int(time.time() * 1000)
                    print(
                        f"\n{datetime.now().isoformat()} - [Measure] (Import Jena {file}): {end-start}ms;")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":

    global_config.host = f'https://{os.environ.get("ARGO_SERVER")}'
    global_config.token = os.environ.get("ARGO_TOKEN")
    global_config.namespace = os.environ.get("ARGO_NAMESPACE", "argo")

    with WorkflowTemplate(
        name="jena-dag",
        entrypoint="jena-dag",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        retry_strategy=RetryStrategy(
            limit=IntOrString(__root__=3),
            retry_policy="Always"
        ),
        arguments=Arguments(parameters=[
            Parameter(name="version",
                      description="Number of versions"),
            Parameter(name="product",
                      description="Number of products"),
            Parameter(name="step", description="Number of steps"),
            Parameter(name="dataset-pvc-name", description="Name of the dataset PVC"),
            Parameter(name="jena-pvc-name", description="Name of the Jena PVC"),
        ])
    ) as wt:
        jena_create = Container(
            name="jena",
            image=constants.jena,
            image_pull_policy=ImagePullPolicy.if_not_present,
            labels={"app": "{{inputs.parameters.jena-name}}"},
            daemon=True,
            inputs=[
                Parameter(name="jena-name"),
                Parameter(name="jena-pvc-name"),
            ],
            security_context=SecurityContext(run_as_user=1000, run_as_group=1000),
            pod_security_context=PodSecurityContext(fs_group=1000),
            init_containers=[
                UserContainer(
                    name="init-jena-data",
                    image=constants.jena,
                    command=["sh", "-c"],
                    security_context=SecurityContext(run_as_user=0),
                    args=[
                        "cp -r /fuseki/. /mnt/fuseki && "
                        "chown -R 1000:1000 /mnt/fuseki && "
                        "echo 'Jena data initialized'"
                    ],
                    volumes=[
                        ExistingVolume(
                            name="{{inputs.parameters.jena-pvc-name}}",
                            claim_name="{{inputs.parameters.jena-pvc-name}}",
                            mount_path="/mnt/fuseki"
                        )
                    ]
                ),
            ],
            env=[
                Env(name="TDB", value="2"),
                Env(name="FUSEKI_DATASET_1", value="mydataset"),
                Env(name="ADMIN_PASSWORD", value=constants.postgres_password),
                Env(name="TINI_SUBREAPER", value="true")
            ],
            resources=Resources(memory_request=f"{constants.memory_request}Gi",
                                memory_limit=f"{constants.memory_limit}Gi", cpu_limit=constants.cpu_limit),
            volumes=[
                ExistingVolume(
                    name="{{inputs.parameters.jena-pvc-name}}",
                    claim_name="{{inputs.parameters.jena-pvc-name}}",
                    mount_path="/fuseki",
                )
            ]
        )

        jena_service_create = Resource(
            name="jena-service",
            inputs=[
                Parameter(name="jena-name"),
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step")
            ],
            action="create",
            manifest=create_service_manifest(
                metadata_name="{{inputs.parameters.jena-name}}-service",
                cleanup=create_cleanup_config(version="{{inputs.parameters.version}}",
                                              product="{{inputs.parameters.product}}",
                                              step="{{inputs.parameters.step}}"),
                selector_name="{{inputs.parameters.jena-name}}",
                port=3030,
                target_port=3030
            )
        )

        querier_create = Container(
            name="jena-querier",
            image=constants.new_quads_querier,
            env=[
                Env(name="ADMIN_PASSWORD", value=constants.postgres_password),
            ],
            inputs=[
                Parameter(name="jena-name"),
                Parameter(name="repeat"),
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step")
            ],
            image_pull_policy=ImagePullPolicy.if_not_present,
            args=["{{inputs.parameters.jena-name}}-service", "jena", "{{inputs.parameters.repeat}}",
                  "{{inputs.parameters.version}}", "{{inputs.parameters.product}}", "{{inputs.parameters.step}}"]
        )

        with DAG(name="jena-dag", inputs=[
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step"),
                Parameter(name="dataset-pvc-name"),
                Parameter(name="jena-pvc-name")]) as dag:
            task_compute_jena_configurations = compute_jena_configurations(
                arguments={
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step"),
                    "workflow_id": "{{workflow.name}}",
                })

            task_jena_create = Task(
                name="jena-create",
                template=jena_create,
                arguments={
                    "jena-name": task_compute_jena_configurations.get_parameter("jena-name"),
                    "jena-pvc-name": dag.get_parameter("jena-pvc-name")
                },
            )

            task_jena_service_create = Task(
                name="jena-service",
                template=jena_service_create,
                arguments={
                    "jena-name": task_compute_jena_configurations.get_parameter("jena-name"),
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step")
                },
            )

            task_jena_importer_create = create_theoretical_dataset_importer(
                name="jena-importer",
                arguments={
                    "existing_volume_name": dag.get_parameter("dataset-pvc-name"),
                    "number_of_versions": dag.get_parameter("version"),
                    "hostname": task_compute_jena_configurations.get_parameter("jena-name")
                }
            )

            task_querier_create = Task(
                name="jena-querier",
                template=querier_create,
                arguments={
                    "jena-name": task_compute_jena_configurations.get_parameter("jena-name"),
                    "repeat": constants.repeat,
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step")
                },
            )

            task_compute_jena_configurations >> task_jena_create >> task_jena_service_create >> task_jena_importer_create >> task_querier_create

        wt.create()
