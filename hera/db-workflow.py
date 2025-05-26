from parse_arguments import parse_arguments
from environment import environment
from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script,
    Task,
    Resource
)
from hera.workflows.models import Toleration, Arguments, Parameter, TemplateRef, ValueFrom
from experiment_utils import create_cleanup_config


@script(inputs=[Parameter(name="db_config")],
        outputs=[Parameter(name="version", value_from=ValueFrom(path="/tmp/version")),
                 Parameter(name="product", value_from=ValueFrom(
                     path="/tmp/product")),
                 Parameter(name="step", value_from=ValueFrom(path="/tmp/step"))])
def prepare_database_config(db_config: object):
    with open("/tmp/version", "w") as f_out:
        f_out.write(f'{db_config.get("version")}')
    with open("/tmp/product", "w") as f_out:
        f_out.write(f'{db_config.get("product")}')
    with open("/tmp/step", "w") as f_out:
        f_out.write(f'{db_config.get("step")}')


if __name__ == "__main__":
    args = parse_arguments()

    environment = environment(args)

    with WorkflowTemplate(
        name="components-dag",
        entrypoint="components-dag",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="db_config",
                      description="Configuration for database", default="{'version': 1, 'product': 1, 'step': 1}"),
            Parameter(name="dataset-pvc-name", description="Name of the dataset PVC", default="pvc-ds-dbs-1"),])
    ) as wt:
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

            task_blazegraph = Task(
                name="blazegraph",
                template_ref=TemplateRef(
                    name="blazegraph-dag", template="blazegraph-dag"),
                arguments=Arguments(
                    parameters=[
                        task_prepare_database_config.get_parameter("version"),
                        task_prepare_database_config.get_parameter("product"),
                        task_prepare_database_config.get_parameter("step"),
                        dag.get_parameter("dataset-pvc-name")
                    ]
                ),
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

            task_prepare_database_config >> [task_blazegraph,
                                     task_converg_condensed, task_converg_flat] >> task_services_removal

        wt.create()
