from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script,
    Task,
    Container,
    Env,
    Resources,
    Resource,
    ExistingVolume
)
from hera.shared import global_config
from hera.workflows.models import Toleration, Arguments, Parameter, ImagePullPolicy, ValueFrom
from experiment_constants import constants
from experiment_utils import create_service_manifest, create_cleanup_config
import os


@script(inputs=[Parameter(name="version"), Parameter(name="product"), Parameter(name="step"), Parameter(name="mode"), Parameter(name="workflow_id")],
        outputs=[Parameter(name="quader-name", value_from=ValueFrom(path="/tmp/quader-name"))])
def compute_quader_configurations(version: str, product: str, step: str, mode: str, workflow_id: str):
    with open("/tmp/quader-name", "w") as f_out:
        f_out.write(f"{workflow_id}-quader-{version}-{product}-{step}-{mode}")

@script(
    image=constants.python_requests,
    inputs=[
        Parameter(name="existing_volume_name", description="The name of the existing volume containing the data to import"),
        Parameter(name="number_of_versions", description="The number of versions to import"),
        Parameter(name="hostname", description="The hostname of the server to import the data into"),
        Parameter(name="mode", description="Configuration for mode")
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
    ]
)
def create_relational_dataset_importer(
    number_of_versions: int,
    hostname: str,
    mode: str
) -> None:
    from datetime import datetime
    import os
    import time
    import requests
    import sys

    directory = "/app/data/data/relational"

    try:
        print(f"Directory: {directory}, Number of versions: {number_of_versions}, Hostname: {hostname}")

        # Get the list of files and directories in the specified directory
        files_and_directories = os.listdir(directory)
        
        # Filter out directories, keeping only files
        files = [f for f in files_and_directories if os.path.isfile(os.path.join(directory, f))]

        # Print the files
        for file in files:
            if file.endswith(".ttl.trig"):
                # Extraire le numéro de version à partir du nom de fichier
                version = int(file.split('-')[-1].split('.ttl')[0])

                # Vérifier si la version est inférieure ou égale au nombre de versions spécifiées
                if version <= number_of_versions:
                    print(f"\n{datetime.now().isoformat()} - [quads-loader] Version {file}")
                    start = int(time.time() * 1000)
                    filepath = os.path.join(directory, file)

                    # Effectuer la requête HTTP pour importer la version
                    try:
                        with open(filepath, 'rb') as f:
                            response = requests.post(
                                f'http://{hostname}-service:8080/import/version',
                                files=dict(file=f)
                            )
                            response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        print(f"Failed to import {filepath}: {e}")
                        sys.exit(1)

                    end = int(time.time() * 1000)
                    print(f"\n{datetime.now().isoformat()} - [Measure] (Import STS {file}): {end-start}ms;")

        if mode == "flat":
            print("[quads-loader] Flattening the dataset")
            response = requests.get(f'http://{hostname}-service:8080/import/flatten')
            response.raise_for_status()
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    
    global_config.host = f'https://{os.environ.get("ARGO_SERVER")}'
    global_config.token = os.environ.get("ARGO_TOKEN")
    global_config.namespace = os.environ.get("ARGO_NAMESPACE", "argo")

    with WorkflowTemplate(
        name="quader-dag",
        entrypoint="quader-dag",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="version",
                      description="Number of versions", default="1"),
            Parameter(name="product",
                      description="Number of products", default="1"),
            Parameter(name="step", description="Number of steps", default="1"),
            Parameter(name="dataset-pvc-name",
                      description="Name of the dataset PVC", default="pvc-ds-dbs-1"),
            Parameter(name="mode", description="Configuration for mode", default="condensed", enum=["condensed", "flat"]),
            Parameter(name="postgres-name", description="Name of the postgres instance", default="postgres"),
            Parameter(name="postgres-identifier", description="Identifier for the postgres instance", default="postgres")])
    ) as wt:

        quader_create = Container(
            name="quader",
            image=constants.quader,
            image_pull_policy=ImagePullPolicy.if_not_present,
            daemon=True,
            labels={"app": "{{inputs.parameters.quader-name}}"},
            inputs=[
                Parameter(name="quader-name"),
                Parameter(name="postgres-name"),
                Parameter(name="postgres-identifier")
            ],
            outputs=[
                Parameter(name="quader-name", value="{{pod.name}}")],
            env=[
                Env(
                    name="SPRING_DATASOURCE_URL",
                    value="jdbc:postgresql://{{inputs.parameters.postgres-name}}-service:5432/{{inputs.parameters.postgres-identifier}}",
                ),
                Env(name="SPRING_DATASOURCE_USERNAME", value=constants.postgres_username),
                Env(name="SPRING_DATASOURCE_PASSWORD", value=constants.postgres_password),
            ],
            resources=Resources(memory_request=f"{constants.memory_request}Gi", memory_limit=f"{constants.memory_limit}Gi", cpu_limit=constants.cpu_limit)
        )

        quader_service_create = Resource(
            name="quader-service",
            inputs=[
                Parameter(name="quader-name"),
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step")
            ],
            action="create",
            manifest=create_service_manifest(
                metadata_name="{{inputs.parameters.quader-name}}-service",
                cleanup=create_cleanup_config(version="{{inputs.parameters.version}}",
                                                product="{{inputs.parameters.product}}",
                                                step="{{inputs.parameters.step}}"),
                selector_name="{{inputs.parameters.quader-name}}",
                port=8080,
                target_port=8080
            )
        )

        with DAG(name="quader-dag", inputs=[
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step"),
                Parameter(name="dataset-pvc-name"),
                Parameter(name="mode"),
                Parameter(name="postgres-name"),
                Parameter(name="postgres-identifier")]) as dag:
            task_compute_quader_configurations = compute_quader_configurations(
                arguments={
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step"),
                    "mode": dag.get_parameter("mode"),
                    "workflow_id": "{{workflow.name}}",
                }
            )

            task_quader_create = Task(
                name="quader",
                template=quader_create,
                arguments={
                    "quader-name": task_compute_quader_configurations.get_parameter("quader-name"),
                    "postgres-name": dag.get_parameter("postgres-name"),
                    "postgres-identifier": dag.get_parameter("postgres-identifier"),
                },
            )

            task_quader_service_create = Task(
                name="quader-service",
                template=quader_service_create,
                arguments={
                    "quader-name": task_compute_quader_configurations.get_parameter("quader-name"),
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step"),
                },
            )

            task_rel_importer = create_relational_dataset_importer(
                name="import-relational-dataset",
                arguments={
                    "existing_volume_name": dag.get_parameter("dataset-pvc-name"),
                    "number_of_versions": dag.get_parameter("version"),
                    "hostname": task_compute_quader_configurations.get_parameter("quader-name"),
                    "mode": dag.get_parameter("mode")
                }
            )

            task_compute_quader_configurations >> task_quader_create >> task_quader_service_create >> task_rel_importer

        wt.create()
