from parse_arguments import parse_arguments
from environment import environment
from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script,
    Task
)
from hera.workflows.models import Toleration, Arguments, Parameter, TemplateRef


@script()
def print_workflow_parameters(parameters: object):
    import json

    print("Printing workflow parameters:")
    print("parameters: ", json.dumps(parameters, sort_keys=True))


@script()
def compute_dbs_dss_configurations(versions: list[int], products: list[int], steps: list[int]):
    """
    Computes the configurations for DSS and DBs.
    This function takes in three lists: versions, products, and steps. It computes the maximum version
    and generates configurations for DSS and DBs based on the provided lists.
    It returns a list of tuples, where each tuple contains the maximum version, product, and step for DSS,
    and a list of tuples for DBs with the same product and step.
    """
    from itertools import product
    import json
    import sys

    dss_configurations = list(product(
        [max(versions)],
        products,
        steps,
    ))

    configurations = list(product(
        versions,
        products,
        steps
    ))

    result = [{
        "ds_config": {
            "version": max_version,
            "product": dss_product,
            "step": dss_step
        },
        "dbs_config": [
            {
                "version": version,
                "product": product,
                "step": step
            }
            for version, product, step in configurations
            if product == dss_product and step == dss_step
        ]
    }
        for max_version, dss_product, dss_step in dss_configurations
    ]

    json.dump(result, sys.stdout)


if __name__ == "__main__":
    args = parse_arguments()

    environment = environment(args)

    with WorkflowTemplate(
        name="converg-xp-main",
        entrypoint="converg-step",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="versions", description="List of versions", default="[1,10,100]"),
            Parameter(name="products", description="List of initial products", default="[1,10]"),
            Parameter(name="steps", description="List of steps between two versions", default="[1,5,10]")]),
    ) as wt:

        with DAG(name="converg-step"):
            task_print_workflow_params = print_workflow_parameters(
                arguments={"parameters": {
                    "versions": "{{workflow.parameters.versions}}",
                    "products": "{{workflow.parameters.products}}",
                    "steps": "{{workflow.parameters.steps}}"
                }})

            task_compute_dbs_dss_configurations = compute_dbs_dss_configurations(
                arguments={"versions": "{{workflow.parameters.versions}}",
                           "products": "{{workflow.parameters.products}}",
                           "steps": "{{workflow.parameters.steps}}"}
            )

            task_ds_dbs = Task(
                name="ds-dbs-loop",
                template_ref=TemplateRef(
                    name="ds-dbs-xp", template="ds-dbs-step"),
                arguments=Arguments(
                    parameters=[Parameter(name="ds_config", value="{{item.ds_config}}"),
                                Parameter(name="dbs_config", value="{{item.dbs_config}}")],
                ),
                with_param=task_compute_dbs_dss_configurations.result
            )

            task_print_workflow_params >> task_compute_dbs_dss_configurations >> task_ds_dbs

        wt.create()
