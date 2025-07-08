from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script,
    Task,
    Container,
    SecretEnv,
    Env,
    Artifact,
)
from hera.shared import global_config
from hera.workflows.models import Toleration, Arguments, Parameter, TemplateRef, ImagePullPolicy
from experiment_constants import constants
import os


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
            "step": dss_step,
        },
        "dbs_config": [
            {
                "version": version,
                "product": product,
                "step": step,
                "db_key": f"v{version}-p{product}-s{step}"
            }
            for version, product, step in configurations
            if product == dss_product and step == dss_step
        ]
    }
        for max_version, dss_product, dss_step in dss_configurations
    ]

    json.dump(result, sys.stdout)


if __name__ == "__main__":

    global_config.host = f'https://{os.environ.get("ARGO_SERVER")}'
    global_config.token = os.environ.get("ARGO_TOKEN")
    global_config.namespace = os.environ.get("ARGO_NAMESPACE", "argo")

    with WorkflowTemplate(
        name="benchmark-dag",
        entrypoint="benchmark-dag",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="versions", description="List of versions", default="[10,50,100]"),
            Parameter(name="products", description="List of initial products (inside the first version)", default="[1]"),
            Parameter(name="steps", description="List of steps between two versions", default="[0,10,20]")])
    ) as wt:
        
        get_workflow_logs = Container(
            name="fetch-workflow-logs",
            image=constants.get_workflow_logs,
            image_pull_policy=ImagePullPolicy.if_not_present,
            inputs=[
                Parameter(name="workflow_id"),
            ],
            env=[
                SecretEnv(name="AWS_SECRET_ACCESS_KEY", secret_name="ceph-s3-pagoda", secret_key="secretkey"),
                SecretEnv(name="AWS_ACCESS_KEY_ID", secret_name="ceph-s3-pagoda", secret_key="accesskey"),
                Env(name="WORKFLOW_ID", value="{{inputs.parameters.workflow_id}}"),
            ],
            outputs=[Artifact(name="time_merged_logs", path="/app/{{inputs.parameters.workflow_id}}/querier/merged_logs.log"),
                     Artifact(name="space_merged_logs", path="/app/{{inputs.parameters.workflow_id}}/space/merged_logs.log")],
        )

        create_time_plots = Container(
            name="time-plots",
            image=constants.log_to_plots,
            inputs=[
                Artifact(name="time_merged_logs", path="/app/merged_logs.log"),
            ],
            image_pull_policy=ImagePullPolicy.if_not_present,
            env=[
                Env(name="LOG_FILE_PATH", value="merged_logs.log"),
            ],
            outputs=[
                Artifact(name="time-plots", path="/app/"),
            ]
        )
        
        create_space_plots = Container(
            name="space-plots",
            image=constants.space_logs_to_plots,
            inputs=[
                Artifact(name="space_merged_logs", path="/app/merged_logs.log"),
            ],
            image_pull_policy=ImagePullPolicy.if_not_present,
            env=[
                Env(name="LOG_FILE_PATH", value="merged_logs.log"),
            ],
            outputs=[
                Artifact(name="space-plots", path="/app/"),
            ]
        )

        with DAG(name="benchmark-dag"):
            task_compute_dbs_dss_configurations = compute_dbs_dss_configurations(
                arguments={"versions": "{{workflow.parameters.versions}}",
                           "products": "{{workflow.parameters.products}}",
                           "steps": "{{workflow.parameters.steps}}"}
            )

            task_ds_dbs = Task(
                name="dataset-databases",
                template_ref=TemplateRef(
                    name="dataset-databases-dag", template="dataset-databases-dag"),
                arguments=Arguments(
                    parameters=[Parameter(name="ds_config", value="{{item.ds_config}}"),
                                Parameter(name="dbs_config", value="{{item.dbs_config}}"),],
                ),
                with_param=task_compute_dbs_dss_configurations.result
            )

            get_workflow_logs_task = Task(
                name="aggregate-workflow-logs",
                template=get_workflow_logs,
                arguments={
                    "workflow_id": "{{workflow.name}}",
                }
            )

            create_time_plots_task = Task(
                name="time-plots",
                template=create_time_plots,
                arguments=get_workflow_logs_task.get_artifact("time_merged_logs"),
            )

            create_space_plots_task = Task(
                name="space-plots",
                template=create_space_plots,
                arguments=get_workflow_logs_task.get_artifact("space_merged_logs"),
            )

            task_compute_dbs_dss_configurations >> task_ds_dbs >> get_workflow_logs_task >> [create_time_plots_task, create_space_plots_task]

        wt.create()
