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
)
from hera.shared import global_config
from hera.workflows.models import Toleration, Arguments, Parameter, ValueFrom, ImagePullPolicy
from experiment_constants import constants
from experiment_utils import create_service_manifest, create_cleanup_config, create_volume_manifest
import os

@script(inputs=[Parameter(name="version"), Parameter(name="product"), Parameter(name="step"), Parameter(name="workflow_id")],
        outputs=[
            Parameter(name="blazegraph-name",
                      value_from=ValueFrom(path="/tmp/blazegraph-name")),
            Parameter(name="blazegraph-pvc-size",
                      value_from=ValueFrom(path="/tmp/blazegraph-pvc-size"))
])
def compute_blazegraph_configurations(version: str, product: str, step: str, workflow_id: str):
    number_of_datasets = 3  # (triples + quads(relational) + quads(graph))
    number_of_version = version + 1
    number_of_step = step + 1
    number_of_triples_one_version = product + number_of_step * number_of_version
    size_one_triple = 0.25  # Mi
    total = 1000 + (number_of_triples_one_version * size_one_triple) * \
        (number_of_version * number_of_datasets)

    with open("/tmp/blazegraph-name", "w") as f_out:
        f_out.write(f"{workflow_id}-blazegraph-{version}-{product}-{step}")
    with open("/tmp/blazegraph-pvc-size", "w") as f_out:
        f_out.write(f'{total}Mi')


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
                    print(
                        f"\n{datetime.now().isoformat()} - [Measure] (Import BG {file}): {end-start}ms;")
    except Exception as e:
        print(f"An error occurred: {e}")


@script(
    image=constants.python_requests,
    inputs=[
        Parameter(name="version", description="The version configuration"),
        Parameter(name="product", description="The product configuration"),
        Parameter(name="step", description="The step configuration"),
        Parameter(name="blazegraph_name",
                  description="The name of the Blazegraph instance"),
        Parameter(name="blazegraph-pvc-name",
                  description="The name of the Blazegraph PVC to bind to the container"),
    ],
    volumes=[
        ExistingVolume(
            name="{{inputs.parameters.blazegraph-pvc-name}}",
            claim_name="{{inputs.parameters.blazegraph-pvc-name}}",
            mount_path="/data"
        )
    ],
    env=[
        Env(name="PYTHONUNBUFFERED", value="1"),
    ]
)
def log_blazegraph_bigdata_size(
    version: str,
    product: str,
    step: str,
    blazegraph_name: str
):
    import os
    import time
    import json
    bigdata_jnl_path = "/data/bigdata.jnl"
    if os.path.exists(bigdata_jnl_path):
        size = os.path.getsize(bigdata_jnl_path)
        now = round(time.time())
        log_entry = {
            "component": blazegraph_name,
            "space": size,
            "version": version,
            "product": product,
            "step": step,
            "time": now
        }
        print(json.dumps(log_entry))


if __name__ == "__main__":

    global_config.host = f'https://{os.environ.get("ARGO_SERVER")}'
    global_config.token = os.environ.get("ARGO_TOKEN")
    global_config.namespace = os.environ.get("ARGO_NAMESPACE", "argo")

    with WorkflowTemplate(
        name="blazegraph-dag",
        entrypoint="blazegraph-dag",
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
        generate_blazegraph_volume = Resource(
            name="blazegraph-volume",
            inputs=[Parameter(name="blazegraph-name"),
                    Parameter(name="blazegraph-pvc-size")],
            outputs=[
                Parameter(name="blazegraph-pvc-name", value_from={"jsonPath": "{.metadata.name}"})],
            action="create",
            set_owner_reference=True,
            manifest=create_volume_manifest('pvc-{{inputs.parameters.blazegraph-name}}', 'ReadWriteOnce', '{{inputs.parameters.blazegraph-pvc-size}}'))

        blazegraph_create = Container(
            name="blazegraph",
            image=constants.blazegraph,
            image_pull_policy=ImagePullPolicy.always,
            labels={"app": "{{inputs.parameters.blazegraph-name}}"},
            daemon=True,
            inputs=[
                Parameter(name="blazegraph-name"),
                Parameter(name="blazegraph-pvc-name"),
            ],
            env=[
                Env(
                    name="BLAZEGRAPH_QUADS",
                    value="true",
                ),
                Env(name="BLAZEGRAPH_TIMEOUT", value=constants.timeout),
                Env(name="BLAZEGRAPH_MEMORY",
                    value=f"{constants.memory_limit}G"),
            ],
            resources=Resources(memory_request=f"{constants.memory_request}Gi",
                                memory_limit=f"{constants.memory_limit}Gi", cpu_limit=constants.cpu_limit),
            volumes=[ExistingVolume(
                name="{{inputs.parameters.blazegraph-pvc-name}}",
                claim_name="{{inputs.parameters.blazegraph-pvc-name}}",
                mount_path="/data"
            )]
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
            args=["{{inputs.parameters.blazegraph-name}}-service", "blazegraph", "{{inputs.parameters.repeat}}",
                  "{{inputs.parameters.version}}", "{{inputs.parameters.product}}", "{{inputs.parameters.step}}"]
        )

        with DAG(name="blazegraph-dag", inputs=[
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step"),
                Parameter(name="dataset-pvc-name")]) as dag:
            task_compute_blazegraph_configurations = compute_blazegraph_configurations(
                arguments={
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step"),
                    "workflow_id": "{{workflow.name}}",
                })

            generate_blazegraph_volume_task = Task(
                name="blazegraph-volume",
                template=generate_blazegraph_volume,
                arguments=Arguments(
                    parameters=[
                        task_compute_blazegraph_configurations.get_parameter(
                            "blazegraph-name"),
                        task_compute_blazegraph_configurations.get_parameter(
                            "blazegraph-pvc-size")
                    ]
                )
            )

            task_blazegraph_create = Task(
                name="blazegraph-create",
                template=blazegraph_create,
                arguments={
                    "blazegraph-name": task_compute_blazegraph_configurations.get_parameter("blazegraph-name"),
                    "blazegraph-pvc-name": generate_blazegraph_volume_task.get_parameter("blazegraph-pvc-name")
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

            task_log_blazegraph_bigdata_size = log_blazegraph_bigdata_size(
                name="blazegraph-bigdata-size-logger",
                arguments={
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step"),
                    "blazegraph_name": task_compute_blazegraph_configurations.get_parameter("blazegraph-name"),
                    "blazegraph-pvc-name": generate_blazegraph_volume_task.get_parameter("blazegraph-pvc-name")
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

            task_compute_blazegraph_configurations >> generate_blazegraph_volume_task >> task_blazegraph_create >> task_blazegraph_service_create >> task_blazegraph_importer_create >> task_log_blazegraph_bigdata_size >> task_querier_create

        wt.create()
