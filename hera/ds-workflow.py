from parse_arguments import parse_arguments
from environment import environment
from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script
)
from hera.workflows.models import Toleration, Arguments, Parameter


@script(inputs=[Parameter(name="ds_config")])
def print_ds_config(ds_config: object):
    import json

    print("Printing ds parameter:")
    print("parameter: ", json.dumps({"ds_config": ds_config}))


if __name__ == "__main__":
    args = parse_arguments()

    environment = environment(args)

    with WorkflowTemplate(
        name="ds-xp",
        entrypoint="ds-step",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="ds_config",
                      description="Configuration for data source", default="{'version': 1, 'product': 1, 'step': 1}")])
    ) as wt:
        with DAG(name="ds-step", inputs=[Parameter(name="ds_config")]) as dag:
            task_print_ds_params = print_ds_config(
                arguments={
                    "ds_config": dag.get_parameter("ds_config")
                })

            task_print_ds_params

        wt.create()
