from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script,
    Task,
    Resource,
    ExistingVolume,
    Env,
)
from hera.shared import global_config
from hera.workflows.models import Toleration, Arguments, Parameter, TemplateRef, ValueFrom
from experiment_utils import create_cleanup_config, create_volume_manifest
from experiment_constants import constants

import os

@script(inputs=[Parameter(name="db_config")],
        outputs=[Parameter(name="version", value_from=ValueFrom(path="/tmp/version")),
                 Parameter(name="product", value_from=ValueFrom(path="/tmp/product")),
                 Parameter(name="step", value_from=ValueFrom(path="/tmp/step")),
                Parameter(name="blazegraph-pvc-size", value_from=ValueFrom(path="/tmp/blazegraph-pvc-size"))])
def prepare_database_config(db_config: dict):
    version = db_config.get("version")
    product = db_config.get("product")
    step = db_config.get("step")

    number_of_datasets = 3  # (triples + quads(relational) + quads(graph))
    number_of_version = version + 1
    number_of_step = step + 1
    number_of_triples_one_version = product + number_of_step * number_of_version
    size_one_triple = 0.25  # Mi
    total = 1000 + (number_of_triples_one_version * size_one_triple) * \
        (number_of_version * number_of_datasets)
    
    with open("/tmp/version", "w") as f_out:
        f_out.write(f'{version}')
    with open("/tmp/product", "w") as f_out:
        f_out.write(f'{product}')
    with open("/tmp/step", "w") as f_out:
        f_out.write(f'{step}')
    with open("/tmp/blazegraph-pvc-size", "w") as f_out:
        f_out.write(f'{total}Mi')


@script(
    image=constants.python_requests,
    inputs=[
        Parameter(name="version", description="The version configuration"),
        Parameter(name="product", description="The product configuration"),
        Parameter(name="step", description="The step configuration"),
        Parameter(name="blazegraph_pvc_name",
                  description="The name of the Blazegraph PVC to bind to the container"),
    ],
    volumes=[
        ExistingVolume(
            name="{{inputs.parameters.blazegraph_pvc_name}}",
            claim_name="{{inputs.parameters.blazegraph_pvc_name}}",
            mount_path="/data"
        )
    ],
    env=[
        Env(name="PYTHONUNBUFFERED", value="1"),
    ]
)
def log_blazegraph_bigdata_space(
    version: str,
    product: str,
    step: str,
):
    import os
    import time
    import json
    bigdata_jnl_path = "/data/bigdata.jnl"
    if os.path.exists(bigdata_jnl_path):
        size = os.path.getsize(bigdata_jnl_path)
        now = round(time.time())
        log_entry = {
            "component": "blazegraph",
            "space": str(size),
            "version": str(version),
            "product": str(product),
            "step": str(step),
            "time": str(now)
        }
        print(json.dumps(log_entry).replace(" ", ""))


if __name__ == "__main__":
    
    global_config.host = f'https://{os.environ.get("ARGO_SERVER")}'
    global_config.token = os.environ.get("ARGO_TOKEN")
    global_config.namespace = os.environ.get("ARGO_NAMESPACE", "argo")

    with WorkflowTemplate(
        name="components-dag",
        entrypoint="components-dag",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="db_config",
                      description="Configuration for database"),
            Parameter(name="dataset-pvc-name", description="Name of the dataset PVC"),])
    ) as wt:
        generate_blazegraph_volume = Resource(
            name="blazegraph-volume",
            inputs=[Parameter(name="blazegraph-pvc-size")],
            outputs=[
                Parameter(name="blazegraph-pvc-name", value_from=ValueFrom(json_path="{.metadata.name}"))],
            action="create",
            set_owner_reference=True,
            manifest=create_volume_manifest('pvc-', 'ReadWriteOnce', '{{inputs.parameters.blazegraph-pvc-size}}'))

        services_removal = Resource(
            name="remove-services",
            inputs=[
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step")
            ],
            action="delete",
            flags=["services", "--selector",
                   f"cleanup={create_cleanup_config(version='{{inputs.parameters.version}}',product='{{inputs.parameters.product}}',step='{{inputs.parameters.step}}')}"]
        )

        with DAG(name="components-dag", inputs=[Parameter(name="db_config"), Parameter(name="dataset-pvc-name")]) as dag:
            task_prepare_database_config = prepare_database_config(
                arguments={
                    "db_config": dag.get_parameter("db_config")
                })
            
            generate_blazegraph_volume_task = Task(
                name="blazegraph-volume",
                template=generate_blazegraph_volume,
                arguments=Arguments(
                    parameters=[
                        task_prepare_database_config.get_parameter("blazegraph-pvc-size")
                    ]
                )
            )

            task_blazegraph = Task(
                name="blazegraph",
                template_ref=TemplateRef(
                    name="blazegraph-dag", template="blazegraph-dag"),
                arguments=Arguments(
                    parameters=[
                        task_prepare_database_config.get_parameter("version"),
                        task_prepare_database_config.get_parameter("product"),
                        task_prepare_database_config.get_parameter("step"),
                        dag.get_parameter("dataset-pvc-name"),
                        generate_blazegraph_volume_task.get_parameter("blazegraph-pvc-name")
                    ]
                ),
            )

            task_log_blazegraph_bigdata_space = log_blazegraph_bigdata_space(
                name="blazegraph-bigdata-space",
                arguments={
                    "version": task_prepare_database_config.get_parameter("version"),
                    "product": task_prepare_database_config.get_parameter("product"),
                    "step": task_prepare_database_config.get_parameter("step"),
                    "blazegraph_pvc_name": generate_blazegraph_volume_task.get_parameter("blazegraph-pvc-name")
                }
            )

            task_converg_condensed = Task(
                name="converg-condensed",
                template_ref=TemplateRef(
                    name="converg-dag", template="converg-dag"),
                arguments=Arguments(
                    parameters=[
                        task_prepare_database_config.get_parameter("version"),
                        task_prepare_database_config.get_parameter("product"),
                        task_prepare_database_config.get_parameter("step"),
                        dag.get_parameter("dataset-pvc-name"),
                        Parameter(name="mode", value="condensed")
                    ]
                ),
            )

            task_converg_flat = Task(
                name="converg-flat",
                template_ref=TemplateRef(
                    name="converg-dag", template="converg-dag"),
                arguments=Arguments(
                    parameters=[
                        task_prepare_database_config.get_parameter("version"),
                        task_prepare_database_config.get_parameter("product"),
                        task_prepare_database_config.get_parameter("step"),
                        dag.get_parameter("dataset-pvc-name"),
                        Parameter(name="mode", value="flat")
                    ]
                ),
            )

            task_services_removal = Task(
                name="remove-services",
                template=services_removal,
                arguments=Arguments(
                    parameters=[
                        task_prepare_database_config.get_parameter("version"),
                        task_prepare_database_config.get_parameter("product"),
                        task_prepare_database_config.get_parameter("step")
                    ]
                ),
            )

            task_prepare_database_config >> generate_blazegraph_volume_task >> [task_blazegraph,
                                     task_converg_condensed, task_converg_flat] >> task_services_removal
            
            task_blazegraph >> task_log_blazegraph_bigdata_space

        wt.create()
