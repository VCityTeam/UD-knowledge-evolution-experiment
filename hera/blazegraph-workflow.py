from parse_arguments import parse_arguments
from environment import environment
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
    Artifact
)
from hera.workflows.models import Toleration, Arguments, Parameter, ValueFrom, ImagePullPolicy
from experiment_constants import constants
from experiment_utils import create_service_manifest, create_cleanup_config

@script(inputs=[Parameter(name="version"), Parameter(name="product"), Parameter(name="step")],
        outputs=[Parameter(name="blazegraph-name", value_from=ValueFrom(path="/tmp/blazegraph-name"))])
def compute_blazegraph_configurations(version: str, product: str, step: str):
    with open("/tmp/blazegraph-name", "w") as f_out:
        f_out.write(f"blazegraph-{version}-{product}-{step}")

@script(
    image=constants.python_requests,
    inputs=[
        Parameter(name="existing_volume_name", description="The name of the existing volume containing the data to import"),
        Parameter(name="number_of_versions", description="The number of versions to import"),
        Parameter(name="hostname", description="The hostname of the server to import the data into"),
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
        print(f"Directory: {directory}, Number of versions: {number_of_versions}, Hostname: {hostname}")

        # Get the list of files and directories in the specified directory
        files_and_directories = os.listdir(directory)
        
        # Filter out directories, keeping only files
        files = [f for f in files_and_directories if os.path.isfile(os.path.join(directory, f))]

        for file in files:
            if file.endswith(".ttl.trig"):
                # Extraire le numéro de version à partir du nom de fichier
                version = int(file.split('-')[-1].split('.ttl')[0])

                # Vérifier si la version est inférieure ou égale au nombre de versions spécifiées
                if version <= number_of_versions:
                    print(f"\n{datetime.now().isoformat()} - [Triple Store] Version {file}")
                    start = int(time.time() * 1000)
                    filepath = os.path.join(directory, file)

                    # Effectuer la requête HTTP pour importer la version
                    try:
                        with open(filepath, 'r') as f:
                            response = requests.post(
                                f'http://{hostname}-service:9999/blazegraph/sparql',
                                headers={'Content-Type': 'application/x-trig'},
                                data=f.read(),
                            )
                            response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        print(f"Failed to import {filepath}: {e}")
                        sys.exit(1)

                    end = int(time.time() * 1000)
                    print(f"\n{datetime.now().isoformat()} - [Measure] (Import BG {file}): {end-start}ms;")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    args = parse_arguments()

    environment = environment(args)

    with WorkflowTemplate(
        name="blazegraph-xp",
        entrypoint="blazegraph-xp",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="version",
                      description="Number of versions", default="1"),
            Parameter(name="product",
                      description="Number of products", default="1"),
            Parameter(name="step", description="Number of steps", default="1"),
            Parameter(name="dataset-pvc-name", description="Name of the dataset PVC", default="pvc-ds-dbs-1"),])
    ) as wt:
        blazegraph_create = Container(
            name="blazegraph",
            image=constants.blazegraph,
            image_pull_policy=ImagePullPolicy.always,
            labels={"app": "{{inputs.parameters.blazegraph-name}}"},
            daemon=True,
            inputs=[
                Parameter(name="blazegraph-name"),
            ],
            env=[
                Env(
                    name="BLAZEGRAPH_QUADS",
                    value="true",
                ),
                Env(name="BLAZEGRAPH_TIMEOUT", value=constants.timeout),
                Env(name="BLAZEGRAPH_MEMORY", value=f"{constants.memory_limit}G"),
            ],
            resources=Resources(memory_request=f"{constants.memory_request}Gi", memory_limit=f"{constants.memory_limit}Gi", cpu_limit=constants.cpu_limit)
        )

        blazegraph_service_create = Resource(
            name="blazegraph-service",
            inputs=[
                Parameter(name="blazegraph-name"),
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step")
            ],
            action="create",
            manifest=create_service_manifest(
                metadata_name="{{inputs.parameters.blazegraph-name}}-service",
                cleanup=create_cleanup_config(version="{{inputs.parameters.version}}",
                                                product="{{inputs.parameters.product}}",
                                                step="{{inputs.parameters.step}}"),
                selector_name="{{inputs.parameters.blazegraph-name}}",
                port=9999,
                target_port=8080
            )
        )

        querier_create = Container(
            name="blazegraph-querier",
            image=constants.new_quads_querier,
            inputs=[
                Parameter(name="blazegraph-name"),
                Parameter(name="repeat"),
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step")
            ],
            image_pull_policy=ImagePullPolicy.always,
            args=["{{inputs.parameters.blazegraph-name}}-service", "blazegraph", "{{inputs.parameters.repeat}}", "{{inputs.parameters.version}}", "{{inputs.parameters.product}}", "{{inputs.parameters.step}}"]
        )

        with DAG(name="blazegraph-xp", inputs=[
            Parameter(name="version"),
            Parameter(name="product"),
            Parameter(name="step"),
            Parameter(name="dataset-pvc-name")]) as dag:            
            task_compute_blazegraph_configurations = compute_blazegraph_configurations(
                arguments={
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step")
                })
            
            task_blazegraph_create = Task(
                name="blazegraph-create",
                template=blazegraph_create,
                arguments={
                    "blazegraph-name": task_compute_blazegraph_configurations.get_parameter("blazegraph-name")
                },
            )

            task_blazegraph_service_create = Task(
                name="blazegraph-service",
                template=blazegraph_service_create,
                arguments={
                    "blazegraph-name": task_compute_blazegraph_configurations.get_parameter("blazegraph-name"),
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step")
                },
            )

            task_blazegraph_importer_create = create_theoretical_dataset_importer(
                name="blazegraph-importer",
                arguments={
                    "existing_volume_name": dag.get_parameter("dataset-pvc-name"),
                    "number_of_versions": dag.get_parameter("version"),
                    "hostname": task_compute_blazegraph_configurations.get_parameter("blazegraph-name")
                }
            )

            task_querier_create = Task(
                name="blazegraph-querier",
                template=querier_create,
                arguments={
                    "blazegraph-name": task_compute_blazegraph_configurations.get_parameter("blazegraph-name"),
                    "repeat": constants.repeat,
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step")
                },
            )

            task_compute_blazegraph_configurations >> task_blazegraph_create >> task_blazegraph_service_create >> task_blazegraph_importer_create >> task_querier_create

        wt.create()
