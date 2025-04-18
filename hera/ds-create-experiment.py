import experiment_layout
from parse_arguments import parse_arguments
from environment import environment
from experiment_constants import constants
from datasets import datasets
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

    experiment_datasets = datasets(layout, environment)

    # Generate the configurations for the datasets (dss is a subset of ds)
    # for a set of configuration having the same product and step, only the maximum version is considered
    # because the generated dataset (with the maximum version) contains all other configuration with the same product and step
    dss_configurations: list[configuration] = experiment_datasets.generate_datasets_configurations(
        parameters)

    with WorkflowTemplate(
        generate_name="converg-ds-create-",
        entrypoint="converg-ds-creator",
        tolerations=[Toleration(
            key="gpu", operator="Exists", effect="PreferNoSchedule")]
    ) as wt:
        # function building all the dataset volumes
        experiment_datasets.create_datasets_volumes(dss_configurations)
        # function building all the dataset containers
        experiment_datasets.create_datasets_generator_containers(
            dss_configurations, constants)
        # function building all the dataset transformers
        experiment_datasets.create_datasets_transformers_containers(
            dss_configurations, constants)

        with DAG(name="converg-ds-creator"):
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

        wt.create()
