import experiment_layout
from parse_arguments import parse_arguments
from environment import environment
from experiment_constants import constants
from databases import databases
from servers import interface_servers
from datasets import datasets, create_relational_dataset_importer, create_theoretical_dataset_importer
from configuration import configuration
from hera.workflows import (
    Task,
    DAG,
    WorkflowTemplate
)
from hera.workflows.models import Toleration

if __name__ == "__main__":
    args = parse_arguments()

    environment = environment(args)
    layout = experiment_layout.layout()

    # Map the arguments to the parameters that shall be used in the workflow
    parameters = {
        "versions": args.versions,
        "products": args.products,
        "steps": args.steps
    }

    experiment_dbs = databases(layout, environment)
    experiment_servers = interface_servers(layout, environment)
    experiment_datasets = datasets(layout, environment)

    # Generate the configurations for the databases and datasets
    dbs_configurations: list[configuration] = experiment_dbs.generate_databases_configurations(
        parameters)
    # Generate the configurations for the datasets (dss is a subset of ds)
    # for a set of configuration having the same product and step, only the maximum version is considered
    # because the generated dataset (with the maximum version) contains all other configuration with the same product and step
    dss_configurations: list[configuration] = experiment_datasets.generate_datasets_configurations(
        parameters)

    with WorkflowTemplate(
        generate_name="converg-xp-",
        entrypoint="converg-xp",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")]
    ) as wt:
        # function building all the database containers/services
        experiment_dbs.create_dbs_containers_services(
            dbs_configurations, constants)
        # function building all the server containers/services
        experiment_servers.create_servers_containers_services(
            dbs_configurations, constants)
        # function building all the logging volumes
        experiment_datasets.create_logging_volumes(
            dbs_configurations)
        # function building all the dataset volumes
        experiment_datasets.create_datasets_volumes(dss_configurations)
        # function building all the dataset containers
        experiment_datasets.create_datasets_generator_containers(
            dss_configurations, constants)
        # function building all the dataset transformers
        experiment_datasets.create_datasets_transformers_containers(
            dss_configurations, constants)
        # function building all the databases queriers
        experiment_dbs.create_dbs_queriers(
            dbs_configurations, constants)
        # function building all services remover
        experiment_dbs.create_services_remover(dbs_configurations)

        with DAG(name="converg-xp"):
            for ds_configuration in dss_configurations:
                # --------------------- Begin DS tasking --------------------- #
                # init all the datasets volumes
                volume_mount = environment.compute_dataset_volume_name(
                    ds_configuration)
                # init all the datasets (bsbm)
                bsbm_container_name = layout.create_bsbm_container_name(
                    ds_configuration)
                # transform the datasets
                relational_transformer_container_name = layout.create_typed_transformer_container_name(
                    ds_configuration, 'relational')
                theoretical_transformer_container_name = layout.create_typed_transformer_container_name(
                    ds_configuration, 'theoretical')

                task_volume = Task(
                    name=f'{volume_mount}-task', template=volume_mount)
                task_dataset_generator = Task(
                    name=f'{bsbm_container_name}-task', template=bsbm_container_name)
                task_relational_transformer = Task(
                    name=f'{relational_transformer_container_name}-task', template=relational_transformer_container_name)
                task_theoretical_transformer = Task(
                    name=f'{theoretical_transformer_container_name}-task', template=theoretical_transformer_container_name)
                # --------------------- End DS tasking --------------------- #

                # --------------------- Begin BSBM workflow --------------------- #
                task_volume >> task_dataset_generator >> [
                    task_relational_transformer, task_theoretical_transformer]
                # --------------------- End BSBM workflow --------------------- #

                # --------------------- Begin DB tasking --------------------- #
                # link the current ds_configuration with a subset of dbs_configuration.
                # This is done by matching the product and step (of the dbs_configuration) with the current ds_configuration
                # we match a ds_configuration to configurations with the same product and step but different versions
                associated_dbs_configurations = experiment_dbs.filter_dbs_configurations_by_ds_configuration(
                    dbs_configurations, ds_configuration)
                for db_configuration in associated_dbs_configurations:
                    # init all the logging volumes
                    logging_volume_mount_name = environment.compute_logging_volume_name(
                        db_configuration)

                    # init all the databases and services (postgresql and blazegraph)
                    postgres_container_name = layout.create_postgres_container_name(
                        db_configuration)
                    postgres_flat_container_name = layout.create_postgres_flat_container_name(
                        db_configuration)
                    blazegraph_container_name = layout.create_blazegraph_container_name(
                        db_configuration)
                    postgres_service_name = layout.create_postgres_service_name(
                        db_configuration)
                    postgres_flat_service_name = layout.create_postgres_flat_service_name(
                        db_configuration)
                    blazegraph_service_name = layout.create_blazegraph_service_name(
                        db_configuration)

                    # create the tasks for the logging volumes
                    task_logging_volume = Task(
                        name=f'{logging_volume_mount_name}-task', template=logging_volume_mount_name)

                    # create the tasks for the databases and their services
                    task_bg_s = Task(
                        name=f'{blazegraph_service_name}-task', template=blazegraph_service_name)
                    task_pg_s = Task(
                        name=f'{postgres_service_name}-task', template=postgres_service_name)
                    task_pg_flat_s = Task(
                        name=f'{postgres_flat_service_name}-task', template=postgres_flat_service_name)
                    task_pg_c = Task(
                        name=f'{postgres_container_name}-task', template=postgres_container_name)
                    task_pg_flat_c = Task(
                        name=f'{postgres_flat_container_name}-task', template=postgres_flat_container_name)
                    task_bg_c = Task(
                        name=f'{blazegraph_container_name}-task', template=blazegraph_container_name)

                    # init all the servers and their associated service (quader and quaque)
                    quader_container_name = layout.create_quader_container_name(
                        db_configuration)
                    quader_flat_container_name = layout.create_quader_flat_container_name(
                        db_configuration)
                    quaque_container_name = layout.create_quaque_container_name(
                        db_configuration)
                    quaque_flat_container_name = layout.create_quaque_flat_container_name(
                        db_configuration)
                    quader_service_name = layout.create_quader_service_name(
                        db_configuration)
                    quader_flat_service_name = layout.create_quader_flat_service_name(
                        db_configuration)
                    quaque_service_name = layout.create_quaque_service_name(
                        db_configuration)
                    quaque_flat_service_name = layout.create_quaque_flat_service_name(
                        db_configuration)

                    # create the tasks for the servers and their services
                    task_quader_s = Task(
                        name=f'{quader_service_name}-task', template=quader_service_name)
                    task_quader_flat_s = Task(
                        name=f'{quader_flat_service_name}-task', template=quader_flat_service_name)
                    task_quaque_s = Task(
                        name=f'{quaque_service_name}-task', template=quaque_service_name)
                    task_quaque_flat_s = Task(
                        name=f'{quaque_flat_service_name}-task', template=quaque_flat_service_name)
                    task_quader_c = Task(
                        name=f'{quader_container_name}-task', template=quader_container_name)
                    task_quader_flat_c = Task(
                        name=f'{quader_flat_container_name}-task', template=quader_flat_container_name)
                    task_quaque_c = Task(
                        name=f'{quaque_container_name}-task', template=quaque_container_name)
                    task_quaque_flat_c = Task(
                        name=f'{quaque_flat_container_name}-task', template=quaque_flat_container_name)

                    # init all the importers (relational and theoretical)
                    relational_importer_container_name = layout.create_typed_importer_container_name(
                        db_configuration, 'relational')
                    relational_flat_importer_container_name = layout.create_typed_importer_container_name(
                        db_configuration, 'relational-flat')
                    theoretical_importer_container_name = layout.create_typed_importer_container_name(
                        db_configuration, 'theoretical')

                    # create the tasks for the condensed importers
                    rel_importer_task = create_relational_dataset_importer(
                        name=f'{relational_importer_container_name}-task',
                        arguments={
                            "python_requests_image": constants.python_requests,
                            "existing_volume_name": environment.compute_dataset_volume_name(ds_configuration),
                            "number_of_versions": db_configuration.version,
                            "hostname": quader_service_name
                        }
                    )

                    # create the tasks for the flat importers
                    rel_flat_importer_task = create_relational_dataset_importer(
                        name=f'{relational_flat_importer_container_name}-task',
                        arguments={
                            "python_requests_image": constants.python_requests,
                            "existing_volume_name": environment.compute_dataset_volume_name(ds_configuration),
                            "number_of_versions": db_configuration.version,
                            "hostname": quader_flat_service_name
                        }
                    )

                    # create the tasks for the theoretical importers
                    theor_importer_task = create_theoretical_dataset_importer(
                        name=f'{theoretical_importer_container_name}-task',
                        arguments={
                            "python_requests_image": constants.python_requests,
                            "existing_volume_name": environment.compute_dataset_volume_name(ds_configuration),
                            "number_of_versions": db_configuration.version,
                            "hostname": blazegraph_service_name
                        }
                    )

                    # create the names for the queriers
                    querier_container_name = layout.create_querier_container_name(
                        db_configuration)

                    # create the tasks for the queriers
                    task_querier = Task(
                        name=f'{querier_container_name}-task', template=querier_container_name)

                    # task to remove all the services
                    delete_services_template_name = layout.create_service_remover_name(
                        db_configuration)
                    task_remove_services = Task(
                        name=f'{delete_services_template_name}-task', template=delete_services_template_name)

                    # --------------------- End DB tasking --------------------- #

                    # --------------------- Begin DB workflow --------------------- #
                    task_logging_volume >> [
                        task_pg_flat_c,
                        task_pg_c,
                        task_bg_c
                    ]

                    task_pg_flat_c >> task_pg_flat_s >> task_quader_flat_c >> task_quader_flat_s >> rel_flat_importer_task
                    task_pg_c >> task_pg_s >> task_quader_c >> task_quader_s >> rel_importer_task
                    task_bg_c >> task_bg_s >> theor_importer_task
                    # --------------------- End DB workflow --------------------- #

                    # --------------------- Begin transformer to importer workflow --------------------- #
                    task_relational_transformer >> [
                        rel_importer_task, rel_flat_importer_task]

                    rel_importer_task >> task_quaque_c >> task_quaque_s >> task_querier
                    rel_flat_importer_task >> task_quaque_flat_c >> task_quaque_flat_s >> task_querier

                    task_theoretical_transformer >> theor_importer_task >> task_querier
                    # --------------------- End transformer to importer workflow --------------------- #

                    task_querier >> task_remove_services

        wt.create()
