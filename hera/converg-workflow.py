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
    Resource,
    ExistingVolume
)
from hera.workflows.models import Toleration, Arguments, Parameter, ImagePullPolicy, ValueFrom, TemplateRef
from experiment_constants import constants
from experiment_utils import create_service_manifest, create_cleanup_config


@script(inputs=[Parameter(name="version"), Parameter(name="product"), Parameter(name="step"), Parameter(name="mode")],
        outputs=[Parameter(name="postgres-name", value_from=ValueFrom(path="/tmp/postgres-name")),
                 Parameter(name="quader-name", value_from=ValueFrom(path="/tmp/quader-name")),
                 Parameter(name="postgres-identifier", value_from=ValueFrom(path="/tmp/postgres-identifier")),
                 Parameter(name="postgres-data", value_from=ValueFrom(path="/tmp/postgres-data"))])
def compute_postgres_configurations(version: str, product: str, step: str, mode: str):
    postgres_name = f"postgres-{mode}-{version}-{product}-{step}"
    quader_name = f"quader-{mode}-{version}-{product}-{step}"
    postgres_identifier = f"postgres-{mode}-{version}-{product}-{step}"
    postgres_data = f"/var/lib/postgresql/data/{version}/{product}/{step}"

    with open("/tmp/postgres-name", "w") as f_out:
        f_out.write(postgres_name)
    with open("/tmp/quader-name", "w") as f_out:
        f_out.write(quader_name)
    with open("/tmp/postgres-identifier", "w") as f_out:
        f_out.write(f'{postgres_identifier}')
    with open("/tmp/postgres-data", "w") as f_out:
        f_out.write(f'{postgres_data}')

if __name__ == "__main__":
    args = parse_arguments()

    environment = environment(args)

    with WorkflowTemplate(
        name="converg-xp",
        entrypoint="converg-xp",
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
            Parameter(name="mode", description="Configuration for mode", default="condensed", enum=["condensed", "flat"])])
    ) as wt:
        postgres_create = Container(
            name="postgres",
            image=constants.postgres,
            image_pull_policy=ImagePullPolicy.always,
            daemon=True,
            labels={"app": "{{inputs.parameters.postgres-name}}"},
            inputs=[
                Parameter(name="postgres-name"),
                Parameter(name="postgres-identifier"),
                Parameter(name="postgres-data")
            ],
            env=[
                Env(
                    name="POSTGRES_DB",
                    value="{{inputs.parameters.postgres-identifier}}",
                ),
                Env(name="POSTGRES_USER", value=constants.postgres_username),
                Env(name="POSTGRES_PASSWORD", value=constants.postgres_password),
                Env(name="PGDATA",
                    value="{{inputs.parameters.postgres-data}}"),
            ],
            resources=Resources(memory_request=f"{constants.memory_request}Gi",
                                memory_limit=f"{constants.memory_limit}Gi", cpu_limit=constants.cpu_limit)
        )

        postgres_service_create = Resource(
            name="postgres-service",
            inputs=[
                Parameter(name="postgres-name"),
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step")
            ],
            action="create",
            manifest=create_service_manifest(
                metadata_name="{{inputs.parameters.postgres-name}}-service",
                cleanup=create_cleanup_config(version="{{inputs.parameters.version}}",
                                                product="{{inputs.parameters.product}}",
                                                step="{{inputs.parameters.step}}"),
                selector_name="{{inputs.parameters.postgres-name}}",
                port=5432,
                target_port=5432
            )
        )


        with DAG(name="converg-xp", inputs=[
                Parameter(name="version"),
                Parameter(name="product"),
                Parameter(name="step"),
                Parameter(name="dataset-pvc-name"),
                Parameter(name="mode")]) as dag:
            task_compute_postgres_configurations = compute_postgres_configurations(
                arguments={
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step"),
                    "mode": dag.get_parameter("mode")
                }
            )

            task_postgres_create = Task(
                name="postgres",
                template=postgres_create,
                arguments={
                    "postgres-name": task_compute_postgres_configurations.get_parameter("postgres-name"),
                    "postgres-identifier": task_compute_postgres_configurations.get_parameter("postgres-identifier"),
                    "postgres-data": task_compute_postgres_configurations.get_parameter("postgres-data"),
                },
            )

            task_postgres_service_create = Task(
                name="postgres-service",
                template=postgres_service_create,
                arguments={
                    "postgres-name": task_compute_postgres_configurations.get_parameter("postgres-name"),
                    "version": dag.get_parameter("version"),
                    "product": dag.get_parameter("product"),
                    "step": dag.get_parameter("step"),
                },
            )

            task_converg_quader = Task(
                name="converg-quader",
                template_ref=TemplateRef(
                    name="quader-xp", template="quader-xp"),
                arguments=Arguments(
                    parameters=[
                        dag.get_parameter("version"),
                        dag.get_parameter("product"),
                        dag.get_parameter("step"),
                        dag.get_parameter("dataset-pvc-name"),
                        dag.get_parameter("mode"),
                        task_compute_postgres_configurations.get_parameter("postgres-name"),
                        task_compute_postgres_configurations.get_parameter("postgres-identifier")
                    ]
                ),
            )

            task_converg_quaque = Task(
                name="converg-quaque",
                template_ref=TemplateRef(
                    name="quaque-xp", template="quaque-xp"),
                arguments=Arguments(
                    parameters=[
                        dag.get_parameter("version"),
                        dag.get_parameter("product"),
                        dag.get_parameter("step"),
                        dag.get_parameter("dataset-pvc-name"),
                        dag.get_parameter("mode"),
                        task_compute_postgres_configurations.get_parameter("postgres-name"),
                        task_compute_postgres_configurations.get_parameter("postgres-identifier")
                    ]
                ),
            )

            task_compute_postgres_configurations >> task_postgres_create >> task_postgres_service_create >> task_converg_quader >> task_converg_quaque

        wt.create()
