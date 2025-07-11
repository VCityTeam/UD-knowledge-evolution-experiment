from hera.workflows import (
    Container,
    Resource,
    Resources,
    Env,
    models,
    Artifact
)
from experiment_utils import create_service_manifest
from experiment_layout import layout
from environment import environment
from configuration import configuration

class interface_servers:
    def __init__(self, layout: layout, environment: environment):
        self.layout = layout
        self.environment = environment

    def create_servers_containers_services(self, configurations: list[configuration], constants) -> None:
        """
        Creates server containers and services based on the provided configurations.
        Args:
            configurations (list[configuration]): A list of configuration objects to create the containers and services.
            constants: A set of constants used during the creation process.
        Returns:
            None
        """
        for configuration in configurations:
            self.create_quader_container_service(configuration, constants)
            self.create_quader_flat_container_service(configuration, constants)
            self.create_quaque_container_service(configuration, constants)
            self.create_quaque_flat_container_service(configuration, constants)

    def create_quader_container_service(self, configuration: configuration, constants) -> None:
        """
        Creates a Quader container and its corresponding Kubernetes service manifest.

        Args:
            configuration (configuration): The configuration object containing necessary parameters.
            constants: An object containing constant values such as image names and credentials.

        Returns:
            None
        """
        quader_container_name = self.layout.create_quader_container_name(configuration)
        quader_service_name = self.layout.create_quader_service_name(configuration)

        Container(
            name=quader_container_name,
            image=constants.quader,
            image_pull_policy=models.ImagePullPolicy.if_not_present,
            daemon=True,
            labels={"app": quader_container_name},
            env=[
                Env(
                    name="SPRING_DATASOURCE_URL",
                    value=self.layout.create_relational_database_url(configuration),
                ),
                Env(name="SPRING_DATASOURCE_USERNAME", value=constants.postgres_username),
                Env(name="SPRING_DATASOURCE_PASSWORD", value=constants.postgres_password),
            ],
            resources=Resources(memory_request=f"{constants.memory_request}Gi", memory_limit=f"{constants.memory_limit}Gi", cpu_limit=constants.cpu_limit),
            outputs=Artifact(
                name=f"{quader_container_name}-log",
                path="/tmp/argo/outputs/logs/main.log"
            )
        )

        manifest = create_service_manifest(quader_service_name, str(configuration), quader_container_name, 8080, 8080)

        Resource(
            name=quader_service_name,
            action="create",
            manifest=manifest,
        )

    def create_quader_flat_container_service(self, configuration: configuration, constants) -> None:
        """
        Creates a Quader flat container and its corresponding Kubernetes service manifest.

        Args:
            configuration (configuration): The configuration object containing necessary parameters.
            constants: An object containing constant values such as image names and credentials.

        Returns:
            None
        """
        quader_flat_container_name = self.layout.create_quader_flat_container_name(configuration)
        quader_flat_service_name = self.layout.create_quader_flat_service_name(configuration)

        Container(
            name=quader_flat_container_name,
            image=constants.quader,
            image_pull_policy=models.ImagePullPolicy.if_not_present,
            daemon=True,
            labels={"app": quader_flat_container_name},
            env=[
                Env(
                    name="SPRING_DATASOURCE_URL",
                    value=self.layout.create_relational_flat_database_url(configuration),
                ),
                Env(name="SPRING_DATASOURCE_USERNAME", value=constants.postgres_username),
                Env(name="SPRING_DATASOURCE_PASSWORD", value=constants.postgres_password),
            ],
            resources=Resources(memory_request=f"{constants.memory_request}Gi", memory_limit=f"{constants.memory_limit}Gi", cpu_limit=constants.cpu_limit),
            outputs=Artifact(
                name=f"{quader_flat_container_name}-log",
                path="/tmp/argo/outputs/logs/main.log"
            )
        )

        manifest = create_service_manifest(quader_flat_service_name, str(configuration), quader_flat_container_name, 8080, 8080)

        Resource(
            name=quader_flat_service_name,
            action="create",
            manifest=manifest,
        )

    def create_quaque_container_service(self, configuration: configuration, constants) -> None:
        """
        Creates a Quaque container service with the specified configuration and constants.

        Args:
            configuration (configuration): The configuration object containing necessary settings.
            constants: An object containing constant values such as image names and database credentials.

        Returns:
            None
        """
        quaque_container_name = self.layout.create_quaque_container_name(configuration)
        quaque_service_name = self.layout.create_quaque_service_name(configuration)

        Container(
            name=quaque_container_name,
            image=constants.quaque,
            image_pull_policy=models.ImagePullPolicy.if_not_present,
            labels={"app": quaque_container_name},
            daemon=True,
            env=[
                Env(name="DATASOURCE_URL", value=self.layout.create_relational_database_url(configuration)),
                Env(name="DATASOURCE_USERNAME", value=constants.postgres_username),
                Env(name="DATASOURCE_PASSWORD", value=constants.postgres_password),
            ],
            resources=Resources(memory_request=f"{constants.memory_request}Gi", memory_limit=f"{constants.memory_limit}Gi", cpu_limit=constants.cpu_limit),
            outputs=Artifact(
                name=f"{quaque_container_name}-log",
                path="/tmp/argo/outputs/logs/main.log"
            )
        )

        manifest = create_service_manifest(quaque_service_name, str(configuration), quaque_container_name, 8081, 8081)

        Resource(
            name=quaque_service_name,
            action="create",
            manifest=manifest,
        )

    def create_quaque_flat_container_service(self, configuration: configuration, constants) -> None:
        """
        Creates a Quaque flat container service with the specified configuration and constants.

        Args:
            configuration (configuration): The configuration object containing necessary settings.
            constants: An object containing constant values such as image names and database credentials.

        Returns:
            None
        """
        quaque_flat_container_name = self.layout.create_quaque_flat_container_name(configuration)
        quaque_flat_service_name = self.layout.create_quaque_flat_service_name(configuration)

        Container(
            name=quaque_flat_container_name,
            image=constants.quaque,
            image_pull_policy=models.ImagePullPolicy.if_not_present,
            labels={"app": quaque_flat_container_name},
            daemon=True,
            env=[
                Env(name="DATASOURCE_URL", value=self.layout.create_relational_flat_database_url(configuration)),
                Env(name="DATASOURCE_USERNAME", value=constants.postgres_username),
                Env(name="DATASOURCE_PASSWORD", value=constants.postgres_password),
                Env(name="CONDENSED_MODE", value="false"),
            ],
            resources=Resources(memory_request=f"{constants.memory_request}Gi", memory_limit=f"{constants.memory_limit}Gi", cpu_limit=constants.cpu_limit),
            outputs=Artifact(
                name=f"{quaque_flat_container_name}-log",
                path="/tmp/argo/outputs/logs/main.log"
            )
        )

        manifest = create_service_manifest(quaque_flat_service_name, str(configuration), quaque_flat_container_name, 8081, 8081)

        Resource(
            name=quaque_flat_service_name,
            action="create",
            manifest=manifest,
        )