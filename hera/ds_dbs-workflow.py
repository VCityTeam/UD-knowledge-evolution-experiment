from parse_arguments import parse_arguments
from environment import environment
from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script
)
from hera.workflows.models import Toleration, Arguments, Parameter


@script(inputs=[Parameter(name="ds_config"), Parameter(name="dbs_config")])
def print_ds_dbs_configs(ds_config: object, dbs_config: list[object]):
    import json

    print("Printing ds dbs parameters:")
    print("parameters: ", json.dumps(
        {"ds_config": ds_config, "dbs_config": dbs_config}, sort_keys=True))


if __name__ == "__main__":
    args = parse_arguments()

    environment = environment(args)

    with WorkflowTemplate(
        name="ds-dbs-xp",
        entrypoint="ds-dbs-step",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="ds_config",
                      description="Configuration for data source", default="{'version': 1, 'product': 1, 'step': 1}"),
            Parameter(name="dbs_config", description="Configurations for databases", default="[{'version': 1, 'product': 1, 'step': 1}, {'version': 10, 'product': 1, 'step': 1}]")])
    ) as wt:
        with DAG(name="ds-dbs-step",
                 inputs=[Parameter(name="ds_config"), Parameter(name="dbs_config")]) as dag:
            task_print_workflow_params = print_ds_dbs_configs(
                arguments={
                    "ds_config": dag.get_parameter("ds_config"),
                    "dbs_config": dag.get_parameter("dbs_config")
                })

            task_print_workflow_params

        wt.create()
