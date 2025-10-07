import json
import re
import os


def get_component_name(component: str):
    """
    Extracts the component name from a given string.
    The component name is the part after the last dot in the string.
    """
    if component.startswith("jena"):
        return "jena"
    if component.startswith("blazegraph"):
        # For blazegraph, we want to keep the full name
        return component
    parts = component.split('-')
    # remove the 3 first elements of parts
    parts = parts[3:]
    component_parts = []
    for part in parts:
        # Check if the part consists only of digits
        if part.isdigit():
            # Stop collecting parts once a numeric part is found
            break
        # Add the non-numeric part to our list
        component_parts.append(part)

    return '-'.join(component_parts)


def extract_log_info(log_file_path: str, min_count_version: int):
    # Définir une expression régulière pour correspondre au format du log
    log_pattern = r'\{"component":"(?P<component>[^"]+)","space":"(?P<space>[^"]+)","version":"(?P<version>[^"]+)","product":"(?P<product>[^"]+)","step":"(?P<step>[^"]+)","time":"(?P<time>[^"]+)"\}'
    extracted_data = []

    # Lire le fichier de logs
    with open(log_file_path, 'r') as file:
        for line in file:
            # Chercher les correspondances avec le pattern
            match = re.search(log_pattern, line)
            if match:
                # Extraire COMPONENT, SPACE, et FILE
                component = match.group('component')
                # keep only the number of the query
                version_conf = int(match.group('version'))
                step_conf = int(match.group('step'))
                # Convertir le temps en millisecondes
                space = int(match.group('space'))
                time_unix = int(match.group('time'))

                extracted_data.append({
                    "VERSION": version_conf,
                    "STEP": step_conf,
                    "COMPONENT": component,
                    "SPACE": space / (1024 * 1024),
                    "TIME": time_unix,
                    "COMPONENT_NAME": get_component_name(component)
                })

    extracted_data = remove_all_with_no_component(data=extracted_data)
    print(f"After remove_all_with_no_component: {len(extracted_data)}")
    extracted_data = remove_all_with_less_than_count_version(data=extracted_data, count=min_count_version)
    print(f"After remove_all_with_less_than_count_version: {len(extracted_data)}")

    return extracted_data

def remove_all_with_no_component(data):
    """
    Remove all entries from data when the metric exists for all components for a given STEP and VERSION
    4 components are expected: blazegraph, jena, quaque-flat, quaque-condensed
    """
    import pandas as pd
    df = pd.DataFrame(data)
    
    component_counts = df.groupby(['STEP', 'VERSION'])['COMPONENT_NAME'].nunique()
    components_to_remove = component_counts[component_counts != 4].index.tolist()
    
    for step, version in components_to_remove:
        df = df[~((df['STEP'] == step) & (df['VERSION'] == version))]
        
    return df.to_dict(orient='records')    
    

def remove_all_with_less_than_count_version(data, count=4):
    """
    Group by STEP, COMPONENT and
    remove all entries from data when the count of version is less than count
    """
    import pandas as pd
    df = pd.DataFrame(data)
    
    # Count the number of unique versions for each group
    version_counts = df.groupby(['STEP', 'COMPONENT_NAME'])['VERSION'].nunique().reset_index()
    version_counts.rename(columns={'VERSION': 'COUNT_VERSION'}, inplace=True)

    # Filter groups with less than count unique versions
    groups_to_remove = version_counts[version_counts['COUNT_VERSION'] < count]

    # Remove all entries from data have the same STEP in groups_to_remove
    for _, row in groups_to_remove.iterrows():
        step, _, _ = row
        df = df[~((df['STEP'] == step))]

    return df.to_dict(orient='records')


def create_space_plot(data, scale="linear"):
    import pandas as pd
    import matplotlib.pyplot as plt

    print("Starting to create space plots.")

    # Read the uploaded JSON file into a pandas DataFrame
    # Use the file handle provided by the environment
    df = pd.DataFrame(data)

    output_dir = f'plots/space/{scale}'
    os.makedirs(output_dir, exist_ok=True)

    # Define the columns that identify a unique configuration
    config_cols = ['STEP', 'COMPONENT_NAME']

    # --- Step 2: Prepare for plotting ---
    # Get unique configurations
    unique_configs = df[config_cols].drop_duplicates().values.tolist()
    num_unique_configs = len(unique_configs)
    print(
        f"Found {num_unique_configs} unique configurations based on {config_cols}.")

    if num_unique_configs == 0:
        print("No data or configurations found to plot.")
    else:
        # associate step to list of components
        config_to_components = {}
        for config in unique_configs:
            step, component = config
            if (step) not in config_to_components:
                config_to_components[(step)] = []
            config_to_components[(step)].append(component)

        # --- Step 3: Generate plots ---
        for (step), components in config_to_components.items():
            fig, ax = plt.subplots(figsize=(12, 6))

            components = sorted(
                components,
                key=lambda x:
                    (x.startswith('blazegraph'), x.startswith('jena'), x.startswith(
                        'quaque-flat'), x.startswith('quaque-condensed')),
                    reverse=True
            )

            # Filter data for the current configuration
            for component in components:
                # Filter the DataFrame for the current configuration
                config_cols = ['STEP', 'COMPONENT_NAME']
                # Create a tuple for the current configuration
                config = [step, component]
                config_filter = (df[config_cols] == pd.Series(
                    config, index=config_cols)).all(axis=1)
                plot_data = df[config_filter].sort_values(by='VERSION')

                # Assign color based on component name
                if component.startswith('blazegraph'):
                    color = 'blue'
                elif component.startswith('jena'):
                    color = 'purple'
                elif component.startswith('postgres-flat'):
                    color = 'orange'
                else:
                    color = 'green'

                ax.plot(plot_data['VERSION'], plot_data['SPACE'],
                        marker='o', linestyle='-', label=component, color=color)

            # Set title and labels
            # Create a multi-line title for better readability
            title_str = f"Space Usage per Version - Step: {step}"
            ax.set_title(title_str, fontsize=9)
            ax.set_xlabel("Version")
            ax.set_ylabel("Space (Mb)")
            if scale == "log":
                ax.set_yscale("log")
                ax.set_ylabel("Space Log (Mb)")

            ax.grid(True)
            # Add legend to the plot
            ax.legend(title='Component', loc='upper left')

            # Ensure x-axis ticks are integers if versions are integers
            ax.xaxis.get_major_locator().set_params(integer=True)

            # --- Create a safe filename for the plot ---
            os.makedirs(f"{output_dir}", exist_ok=True)
            filepath = f"{output_dir}/space-{step}.png"
            plt.savefig(filepath, dpi=300)
            plt.close(fig)

def create_space_csv(data):
    # remove the column "COMPONENT", "TIME"
    output_dir = f'plots/space/csv'
    os.makedirs(output_dir, exist_ok=True)
    import pandas as pd
    df = pd.DataFrame(data)
    df = df.drop(columns=["COMPONENT", "TIME"], errors='ignore')
    
    # for each STEP, VERSION, display the space of each component in a separate column
    df_pivot = df.pivot_table(index=['STEP', 'VERSION'], columns='COMPONENT_NAME', values='SPACE').reset_index()
    df_pivot = df_pivot.sort_values(by=['STEP', 'VERSION'])
    
    # precision to 2 decimals
    df_pivot = df_pivot.round(2)

    df_pivot.to_csv(f"{output_dir}/space.csv", index=False)


def store_data_to_json(data, file_path):
    """
    Store the data to a json file
    Args:
        data (list): List of dictionaries
        file_path (str): Path to the json file
    """
    with open(file_path, 'w') as f:
        json.dump(data, f)

if __name__ == "__main__":
    # Afficher les informations extraites

    log_file_path = os.getenv("LOG_FILE_PATH", "merged_logs.log")
    min_count_version = int(os.getenv("COUNT_VERSION", 3))

    if not log_file_path:
        raise EnvironmentError(
            "LOG_FILE_PATH environment variable is not set.")

    print(f"Log file path: {log_file_path}")
    print(f"Minimum count version: {min_count_version}")

    log_data = extract_log_info(log_file_path, min_count_version)
    
    store_data_to_json(data=log_data, file_path="log_data.json")

    for scale in ["linear", "log"]:
        create_space_plot(data=log_data, scale=scale)
    create_space_csv(data=log_data)
 