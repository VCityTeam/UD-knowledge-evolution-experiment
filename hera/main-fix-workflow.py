from parse_arguments import parse_arguments
from environment import environment
from hera.workflows import (
    DAG,
    Workflow,
    script,
    Task,
    Container,
    SecretEnv,
    Env,
    Artifact
)
from hera.workflows.models import Toleration, Arguments, Parameter, TemplateRef, ImagePullPolicy
from experiment_constants import constants


@script()
def compute_dbs_dss_configurations():
    """
    Computes the configurations for DSS and DBs.
    It returns a list of tuples, where each tuple contains the maximum version, product, and step for DSS,
    and a list of tuples for DBs with the same product and step.
    """
    import json
    import sys

    result = [
        {
            "ds_config": {
                "version": 100,
                "product": 20,
                "step": 20,
            },
            "dbs_config": [
                {
                    "version": 5,
                    "product": 20,
                    "step": 20,
                    "db_key": f"v{5}-p{20}-s{20}"
                },
                {
                    "version": 35,
                    "product": 20,
                    "step": 20,
                    "db_key": f"v{35}-p{20}-s{20}"
                },
                {
                    "version": 70,
                    "product": 20,
                    "step": 20,
                    "db_key": f"v{70}-p{20}-s{20}"
                },
                {
                    "version": 100,
                    "product": 20,
                    "step": 20,
                    "db_key": f"v{100}-p{20}-s{20}"
                },
            ]
        },
        {
            "ds_config": {
                "version": 100,
                "product": 20,
                "step": 0,
            },
            "dbs_config": [
                {
                    "version": 70,
                    "product": 20,
                    "step": 0,
                    "db_key": f"v{70}-p{20}-s{0}"
                }
            ]
        },
        {
            "ds_config": {
                "version": 100,
                "product": 10,
                "step": 10,
            },
            "dbs_config": [
                {
                    "version": 100,
                    "product": 10,
                    "step": 10,
                    "db_key": f"v{100}-p{10}-s{10}"
                }
            ]
        },
        {
            "ds_config": {
                "version": 100,
                "product": 1,
                "step": 10,
            },
            "dbs_config": [
                {
                    "version": 100,
                    "product": 1,
                    "step": 10,
                    "db_key": f"v{100}-p{1}-s{10}"
                },
                {
                    "version": 35,
                    "product": 1,
                    "step": 10,
                    "db_key": f"v{35}-p{1}-s{10}"
                },
            ]
        },
    ]

    json.dump(result, sys.stdout)


if __name__ == "__main__":
    args = parse_arguments()

    environment = environment(args)

    with Workflow(
        name="workflow-xp-main",
        entrypoint="workflow-step",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="versions", description="List of versions",
                      default="[10,50,100]"),
            Parameter(
                name="products", description="List of initial products", default="[1,20,40]"),
            Parameter(name="steps", description="List of steps between two versions", default="[0,10,20]")])
    ) as wt:

        get_workflow_logs = Container(
            name="fetch-workflow-logs",
            image=constants.get_workflow_logs,
            image_pull_policy=ImagePullPolicy.always,
            inputs=[
                Parameter(name="workflow_id"),
            ],
            env=[
                SecretEnv(name="AWS_SECRET_ACCESS_KEY",
                          secret_name="ceph-s3-pagoda", secret_key="secretkey"),
                SecretEnv(name="AWS_ACCESS_KEY_ID",
                          secret_name="ceph-s3-pagoda", secret_key="accesskey"),
                Env(name="WORKFLOW_ID",
                    value="{{inputs.parameters.workflow_id}}"),
            ],
            outputs=Artifact(
                name="merged_logs", path="/app/{{inputs.parameters.workflow_id}}/merged_logs.log"),
        )

        create_plots = Container(
            name="plots",
            image=constants.log_to_plots,
            inputs=[
                Artifact(name="merged_logs", path="/app/merged_logs.log"),
            ],
            image_pull_policy=ImagePullPolicy.always,
            env=[
                Env(name="LOG_FILE_PATH", value="merged_logs.log"),
            ],
            outputs=[
                Artifact(name="plots", path="/app/"),
            ]
        )

        with DAG(name="workflow-step"):
            task_compute_dbs_dss_configurations = compute_dbs_dss_configurations()

            task_ds_dbs = Task(
                name="dataset-databases",
                template_ref=TemplateRef(
                    name="dataset-databases-xp", template="dataset-databases-xp"),
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
                    "workflow_id": "{{workflow.name}}/",
                }
            )

            create_plots_task = Task(
                name="plots",
                template=create_plots,
                arguments=get_workflow_logs_task.get_artifact("merged_logs"),
            )

            task_compute_dbs_dss_configurations >> task_ds_dbs >> get_workflow_logs_task >> create_plots_task

        wt.create()
