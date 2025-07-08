from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script,
    Container,
    Task,
    ExistingVolume
)
from hera.shared import global_config
from hera.workflows.models import Toleration, Arguments, Parameter, ImagePullPolicy, ValueFrom
from experiment_constants import constants
import os


@script(inputs=[Parameter(name="ds_config")],
        outputs=[Parameter(name="version", value_from=ValueFrom(path="/tmp/version")),
                 Parameter(name="product", value_from=ValueFrom(path="/tmp/product")),
                 Parameter(name="step", value_from=ValueFrom(path="/tmp/step"))])
def prepare_dataset_config(ds_config: dict):   
    with open("/tmp/version", "w") as f_out:
        f_out.write(str(ds_config.get('version')))
    with open("/tmp/product", "w") as f_out:
        f_out.write(str(ds_config.get('product')))
    with open("/tmp/step", "w") as f_out:
        f_out.write(str(ds_config.get('step')))


if __name__ == "__main__":

    global_config.host = f'https://{os.environ.get("ARGO_SERVER")}'
    global_config.token = os.environ.get("ARGO_TOKEN")
    global_config.namespace = os.environ.get("ARGO_NAMESPACE", "argo")

    with WorkflowTemplate(
        name="dataset-dag",
        entrypoint="dataset-dag",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="ds_config",
                      description="Configuration for data source", default="{'version': 1, 'product': 1, 'step': 1}"),
            Parameter(name="dataset-pvc-name", description="Key for data source", default="v1_p1_s1")])
    ) as wt:
        dataset_generator = Container(
            name="generate-dataset",
            image=constants.bsbm,
            inputs=[Parameter(name="version"), Parameter(name="product"), Parameter(name="step"), Parameter(name="dataset-pvc-name")],
            volumes=[ExistingVolume(
                name='{{inputs.parameters.dataset-pvc-name}}',
                claim_name='{{inputs.parameters.dataset-pvc-name}}',
                mount_path="/app/data",
            )],
            image_pull_policy=ImagePullPolicy.if_not_present,
            args=["generate-n", "{{inputs.parameters.version}}", "{{inputs.parameters.product}}", "{{inputs.parameters.step}}"],
        )

        relational_transformer = Container(
            name="relational-transformer",
            image=constants.quads_transformer,
            inputs=[Parameter(name="dataset-pvc-name")],
            volumes=[ExistingVolume(
                name='{{inputs.parameters.dataset-pvc-name}}',
                claim_name='{{inputs.parameters.dataset-pvc-name}}',
                mount_path="/app/data",
            )],
            image_pull_policy=ImagePullPolicy.if_not_present,
             args=[
                f"/app/data/data/relational",
                f"/app/data/data",
                "*",
                "relational",
                "BSBM"
            ],
        )

        theoretical_transformer = Container(
            name="theoretical-transformer",
            image=constants.quads_transformer,
            inputs=[Parameter(name="dataset-pvc-name")],
            volumes=[ExistingVolume(
                name='{{inputs.parameters.dataset-pvc-name}}',
                claim_name='{{inputs.parameters.dataset-pvc-name}}',
                mount_path="/app/data",
            )],
            image_pull_policy=ImagePullPolicy.if_not_present,
             args=[
                f"/app/data/data/theoretical",
                f"/app/data/data",
                "*",
                "theoretical",
                "BSBM"
            ],
        )

        with DAG(name="dataset-dag", inputs=[Parameter(name="ds_config"), Parameter(name="dataset-pvc-name")]) as dag:
            task_prepare_dataset_config = prepare_dataset_config(
                arguments={
                    "ds_config": dag.get_parameter("ds_config")
                })

            task_dataset_generator = Task(
                name="generate-dataset",
                template=dataset_generator,
                arguments=Arguments(
                    parameters=[
                        task_prepare_dataset_config.get_parameter("version"),
                        task_prepare_dataset_config.get_parameter("product"),
                        task_prepare_dataset_config.get_parameter("step"),
                        dag.get_parameter("dataset-pvc-name")
                    ]
                )
            )

            task_relational_transformer = Task(
                name="relational-transformer",
                template=relational_transformer,
                arguments=Arguments(
                    parameters=[
                        dag.get_parameter("dataset-pvc-name")
                    ]
                )
            )

            task_theoretical_transformer = Task(
                name="theoretical-transformer",
                template=theoretical_transformer,
                arguments=Arguments(
                    parameters=[
                        dag.get_parameter("dataset-pvc-name")
                    ]
                )
            )

            task_prepare_dataset_config >> task_dataset_generator >> [task_relational_transformer, task_theoretical_transformer]

        wt.create()
