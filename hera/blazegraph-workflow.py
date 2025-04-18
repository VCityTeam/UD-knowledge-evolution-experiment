from parse_arguments import parse_arguments
from environment import environment
from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script
)
from hera.workflows.models import Toleration, Arguments, Parameter


@script(inputs=[Parameter(name="db_config"), Parameter(name="ds_config")])
def print_blazegraph_config(db_config: object, ds_config: object):
    import json

    print("Printing blazegraph parameter:")
    print("parameter: ", json.dumps({"db_config": db_config, "ds_config": ds_config}))


if __name__ == "__main__":
    args = parse_arguments()

    environment = environment(args)

    with WorkflowTemplate(
        name="blazegraph-xp",
        entrypoint="blazegraph-step",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="db_config",
                      description="Configuration for database", default="{'version': 1, 'product': 1, 'step': 1}"),
            Parameter(name="ds_config", description="Configuration for data source", default="{'version': 5, 'product': 1, 'step': 1}"),])
    ) as wt:
        with DAG(name="blazegraph-step", inputs=[Parameter(name="db_config"), Parameter(name="ds_config")]) as dag:
            task_print_blazegraph_params = print_blazegraph_config(
                arguments={
                    "db_config": dag.get_parameter("db_config"),
                    "ds_config": dag.get_parameter("ds_config")
                })

            task_print_blazegraph_params

        wt.create()
