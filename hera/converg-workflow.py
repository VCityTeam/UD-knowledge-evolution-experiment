from parse_arguments import parse_arguments
from environment import environment
from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script
)
from hera.workflows.models import Toleration, Arguments, Parameter


@script(inputs=[Parameter(name="db_config"), Parameter(name="ds_config"), Parameter(name="mode")])
def print_converg_config(db_config: object, ds_config: object, mode: str):
    import json

    print("Printing converg parameter:")
    print("parameter: ", json.dumps({"db_config": db_config, "ds_config": ds_config, "mode": mode}))


if __name__ == "__main__":
    args = parse_arguments()

    environment = environment(args)

    with WorkflowTemplate(
        name="converg-xp",
        entrypoint="converg-step",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="db_config",
                      description="Configuration for database", default="{'version': 1, 'product': 1, 'step': 1}"),
            Parameter(name="ds_config", description="Configuration for data source", default="{'version': 5, 'product': 1, 'step': 1}"),
            Parameter(name="mode", description="Configuration for mode", default="condensed", enum=["condensed", "flat"])])
    ) as wt:
        with DAG(name="converg-step", inputs=[Parameter(name="db_config"), Parameter(name="ds_config"), Parameter(name="mode")]) as dag:
            task_print_converg_params = print_converg_config(
                arguments={
                    "db_config": dag.get_parameter("db_config"),
                    "ds_config": dag.get_parameter("ds_config"),
                    "mode": dag.get_parameter("mode")
                })

            task_print_converg_params

        wt.create()
