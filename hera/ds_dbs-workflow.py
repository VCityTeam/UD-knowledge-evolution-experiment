from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script,
    Task,
    Resource
)
from hera.shared import global_config
from hera.workflows.models import (
    Toleration, Arguments, Parameter, TemplateRef, ValueFrom)
from experiment_utils import create_volume_manifest
import os

@script(inputs=[Parameter(name="ds_config")], outputs=[Parameter(name="pvc-name", value_from=ValueFrom(path="/tmp/pvc-name")), Parameter(name="pvc-size", value_from=ValueFrom(path="/tmp/pvc-size"))])
def compute_pvc_config(ds_config: dict):
    number_of_datasets = 3  # (triples + quads(relational) + quads(graph))
    number_of_version = ds_config.get("version") + 1
    number_of_step = ds_config.get("step") + 1
    number_of_triples_one_version = ds_config.get(
        "product") + number_of_step * number_of_version
    size_one_triple = 0.25  # Mi
    total = (number_of_triples_one_version * size_one_triple) * \
        (number_of_version * number_of_datasets)
    with open("/tmp/pvc-size", "w") as f_out:
        f_out.write(f'{total}Mi')
    with open("/tmp/pvc-name", "w") as f_out:
        f_out.write(f'pvc-ds-dbs-v{ds_config.get("version")}-p{ds_config.get("product")}-s{ds_config.get("step")}-')

if __name__ == "__main__":

    global_config.host = f'https://{os.environ.get("ARGO_SERVER")}'
    global_config.token = os.environ.get("ARGO_TOKEN")
    global_config.namespace = os.environ.get("ARGO_NAMESPACE", "argo")

    with WorkflowTemplate(
        name="dataset-databases-dag",
        entrypoint="dataset-databases-dag",
        parallelism=3,
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="ds_config",
                      description="Configuration for data source", default="{'version': 1, 'product': 1, 'step': 1}"),
            Parameter(name="dbs_config", description="Configurations for databases", default="[{'version': 1, 'product': 1, 'step': 1}, {'version': 10, 'product': 1, 'step': 1}]")])
    ) as wt:
        generate_volume = Resource(
            name="dataset-volume",
            inputs=[Parameter(name="pvc-name"), Parameter(name="pvc-size")],
            outputs=[
                Parameter(name="dataset-pvc-name", value_from={"jsonPath": "{.metadata.name}"})],
            action="create",
            set_owner_reference=True,
            manifest=create_volume_manifest('{{inputs.parameters.pvc-name}}', 'ReadWriteOnce', '{{inputs.parameters.pvc-size}}'))
        with DAG(name="dataset-databases-dag",
                 inputs=[Parameter(name="ds_config"), Parameter(name="dbs_config")]) as dag:
            compute_pvc_config_task = compute_pvc_config(
                arguments={
                    "ds_config": dag.get_parameter("ds_config")
                }
            )

            generate_volume_task = Task(
                name="dataset-volume",
                template=generate_volume,
                arguments=Arguments(
                    parameters=[
                        compute_pvc_config_task.get_parameter("pvc-size"),
                        compute_pvc_config_task.get_parameter("pvc-name")
                    ]
                )
            )

            task_ds = Task(
                name="generate-complete-dataset",
                template_ref=TemplateRef(
                    name="dataset-dag", template="dataset-dag"),
                arguments=Arguments(
                    parameters=[dag.get_parameter("ds_config"),
                                generate_volume_task.get_parameter("dataset-pvc-name")],
                ),
            )

            task_dbs = Task(
                name="components-dag",
                template_ref=TemplateRef(
                    name="components-dag", template="components-dag"),
                arguments=Arguments(
                    parameters=[
                        Parameter(name="db_config", value="{{item}}"),
                        generate_volume_task.get_parameter("dataset-pvc-name")]
                ),
                with_param=dag.get_parameter("dbs_config")
            )

            compute_pvc_config_task >> generate_volume_task >> task_ds >> task_dbs

        wt.create()
