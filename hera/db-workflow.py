from parse_arguments import parse_arguments
from environment import environment
from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script,
    Task
)
from hera.workflows.models import Toleration, Arguments, Parameter, TemplateRef


@script(inputs=[Parameter(name="db_config"), Parameter(name="ds_config")])
def print_db_config(db_config: object, ds_config: object):
    import json

    print("Printing db and ds parameter:")
    print("parameter: ", json.dumps({"db_config": db_config, "ds_config": ds_config}))


if __name__ == "__main__":
    args = parse_arguments()

    environment = environment(args)

    with WorkflowTemplate(
        name="db-xp",
        entrypoint="db-step",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="db_config",
                      description="Configuration for database", default="{'version': 1, 'product': 1, 'step': 1}"),
            Parameter(name="ds_config", description="Configuration for data source", default="{'version': 5, 'product': 1, 'step': 1}"),])
    ) as wt:
        with DAG(name="db-step", inputs=[Parameter(name="db_config"), Parameter(name="ds_config")]) as dag:
            task_print_db_params = print_db_config(
                arguments={
                    "db_config": dag.get_parameter("db_config"),
                    "ds_config": dag.get_parameter("ds_config")
                })
            
            task_blazegraph = Task(
                name="blazegraph-instance",
                template_ref=TemplateRef(
                    name="blazegraph-xp", template="blazegraph-step"),
                arguments=Arguments(
                    parameters=[
                        dag.get_parameter("ds_config"),
                        dag.get_parameter("db_config")
                    ]
                ),
            )

            task_converg_condensed = Task(
                name="converg-condensed-instance",
                template_ref=TemplateRef(
                    name="converg-xp", template="converg-step"),
                arguments=Arguments(
                    parameters=[
                        dag.get_parameter("ds_config"),
                        dag.get_parameter("db_config"),
                        Parameter(name="mode", value="condensed")
                    ]
                ),
            )

            task_converg_flat = Task(
                name="converg-flat-instance",
                template_ref=TemplateRef(
                    name="converg-xp", template="converg-step"),
                arguments=Arguments(
                    parameters=[
                        dag.get_parameter("ds_config"),
                        dag.get_parameter("db_config"),
                        Parameter(name="mode", value="flat")
                    ]
                ),
            )

            task_print_db_params >> [task_blazegraph, task_converg_condensed, task_converg_flat]

        wt.create()
