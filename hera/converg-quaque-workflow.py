from parse_arguments import parse_arguments
from environment import environment
from hera.workflows import (
    DAG,
    WorkflowTemplate,
    script,
    Task,
    Container,
    Env,
    Resources,
    Resource
)
from hera.workflows.models import Toleration, Arguments, Parameter, ImagePullPolicy, ValueFrom
from experiment_constants import constants
from experiment_utils import create_service_manifest, create_cleanup_config


@script(inputs=[Parameter(name="version"), Parameter(name="product"), Parameter(name="step"), Parameter(name="mode")],
        outputs=[Parameter(name="quaque-name", value_from=ValueFrom(path="/tmp/quaque-name"))])
def compute_quaque_configurations(version: str, product: str, step: str, mode: str):
    with open("/tmp/quaque-name", "w") as f_out:
        f_out.write(f"quaque-{mode}-{version}-{product}-{step}")


if __name__ == "__main__":
    args = parse_arguments()

    environment = environment(args)

    with WorkflowTemplate(
        name="quaque-xp",
        entrypoint="quaque-xp",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")],
        arguments=Arguments(parameters=[
            Parameter(name="version",
                      description="Number of versions", default="1"),
            Parameter(name="product",
                      description="Number of products", default="1"),
            Parameter(name="step", description="Number of steps", default="1"),
            Parameter(name="dataset-pvc-name",
                      description="Name of the dataset PVC", default="pvc-ds-dbs-1"),
            Parameter(name="mode", description="Configuration for mode",
                      default="condensed", enum=["condensed", "flat"]),
            Parameter(name="postgres-name",
                      description="Name of the postgres instance", default="postgres"),
            Parameter(name="postgres-identifier", description="Identifier for the postgres instance", default="postgres")])
    ) as wt:
        quaque_create = Container(
            name="quaque",
            image=constants.quaque,
            image_pull_policy=ImagePullPolicy.always,
            labels={"app": "{{inputs.parameters.quaque-name}}"},
            daemon=True,
            env=[
                Env(name="DATASOURCE_URL",
                    value="jdbc:postgresql://{{inputs.parameters.postgres-name}}-service:5432/{{inputs.parameters.postgres-identifier}}"),
                Env(name="DATASOURCE_USERNAME",
                    value=constants.postgres_username),
                Env(name="DATASOURCE_PASSWORD",
                    value=constants.postgres_password),
            ],
            resources=Resources(memory_request=f"{constants.memory_request}Gi",
                                memory_limit=f"{constants.memory_limit}Gi", cpu_limit=constants.cpu_limit),
            inputs=[
                Parameter(name="quaque-name"),
                Parameter(name="postgres-name"),
                Parameter(name="postgres-identifier")
            ]
        )

        quaque_service_create = Resource(
            name="quaque-service",
            inputs=[
                Parameter(name="quaque-name"),
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step")
            ],
            action="create",
            manifest=create_service_manifest(
                metadata_name="{{inputs.parameters.quaque-name}}-service",
                cleanup=create_cleanup_config(version="{{inputs.parameters.version}}",
                                              product="{{inputs.parameters.product}}",
                                              step="{{inputs.parameters.step}}"),
                selector_name="{{inputs.parameters.quaque-name}}",
                port=8081,
                target_port=8081
            )
        )

        querier_create = Container(
            name="quaque-querier",
            image=constants.new_quads_querier,
            inputs=[
                Parameter(name="quaque-name"),
                Parameter(name="repeat"),
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step")
            ],
            image_pull_policy=ImagePullPolicy.always,
            args=["{{inputs.parameters.quaque-name}}-service", "converg", "{{inputs.parameters.repeat}}", "{{inputs.parameters.version}}", "{{inputs.parameters.product}}", "{{inputs.parameters.step}}"]
        )

        with DAG(name="quaque-xp", inputs=[
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step"),
                Parameter(name="dataset-pvc-name"),
                Parameter(name="mode"),
                Parameter(name="postgres-name"),
                Parameter(name="postgres-identifier")]) as dag:

            task_compute_quaque_configurations = compute_quaque_configurations(
                arguments={
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step"),
                    "mode": dag.get_parameter("mode")
                }
            )

            task_quaque_create = Task(
                name="quaque",
                template=quaque_create,
                arguments={
                    "quaque-name": task_compute_quaque_configurations.get_parameter("quaque-name"),
                    "postgres-name": dag.get_parameter("postgres-name"),
                    "postgres-identifier": dag.get_parameter("postgres-identifier"),
                },
            )

            task_quaque_service_create = Task(
                name="quaque-service",
                template=quaque_service_create,
                arguments={
                    "quaque-name": task_compute_quaque_configurations.get_parameter("quaque-name"),
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step"),
                },
            )

            task_querier_create = Task(
                name="quaque-querier",
                template=querier_create,
                arguments={
                    "quaque-name": task_compute_quaque_configurations.get_parameter("quaque-name"),
                    "repeat": constants.repeat,
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step")
                },
            )

            task_compute_quaque_configurations >> task_quaque_create >> task_quaque_service_create >> task_querier_create

        wt.create()
