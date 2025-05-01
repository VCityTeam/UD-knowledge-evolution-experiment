import re
import os

def get_component_name(component: str):
    """
    Extracts the component name from a given string.
    The component name is the part after the last dot in the string.
    """
    parts = component.split('-')
    component_parts = []
    for part in parts:
        # Check if the part consists only of digits
        if part.isdigit():
            # Stop collecting parts once a numeric part is found
            break
        # Add the non-numeric part to our list
        component_parts.append(part)

    return '-'.join(component_parts)

def extract_log_info(log_file_path: str):
    # Définir une expression régulière pour correspondre au format du log
    log_pattern = r'\{"component":"(?P<component>[^"]+)","query":"(?P<query>[^"]+)","try":"(?P<try>[^"]+)","duration":"(?P<duration>[^"]+)","version":"(?P<version>[^"]+)","product":"(?P<product>[^"]+)","step":"(?P<step>[^"]+)","time":"(?P<time>[^"]+)"\}'
    extracted_data = []

    # Lire le fichier de logs
    with open(log_file_path, 'r') as file:
        for line in file:
            # Chercher les correspondances avec le pattern
            match = re.search(log_pattern, line)
            if match:
                # Extraire COMPONENT, DURATION, et FILE
                component = match.group('component')
                # keep only the number of the query
                query = f"query-{match.group('query').split('-')[-1].split('.')[0]}"
                nb_try = int(match.group('try'))
                version_conf = int(match.group('version'))
                product_conf = int(match.group('product'))
                step_conf = int(match.group('step'))
                # Convertir le temps en millisecondes
                duration_ms = int(match.group('duration').replace("ms", ""))
                time_unix = int(match.group('time'))

                extracted_data.append({
                    "VERSION": version_conf,
                    "PRODUCT": product_conf,
                    "STEP": step_conf,
                    "COMPONENT": component,
                    "DURATION (ms)": duration_ms,
                    "QUERY": query,
                    "TRY": nb_try,
                    "TIME": time_unix,
                    "COMPONENT_NAME": get_component_name(component)
                })

    return extracted_data

def whisker_duration_per_component_query_config(data, limit=None):
    import pandas as pd
    import matplotlib.pyplot as plt
    import os
    import numpy as np
    
    grouping_cols = ['VERSION', 'PRODUCT', 'STEP', 'QUERY']
    df = pd.DataFrame(data)
    if limit is not None:
        df = df[df['TRY'] >= limit]
    output_dir = 'plots/whiskers'
    os.makedirs(output_dir, exist_ok=True)
    grouped_data = df.groupby(grouping_cols)
    print(f"Found {len(grouped_data)} groups based on {grouping_cols}.")

    for name, group in grouped_data:
        # Extract group keys
        version, product, step, query = name
        grouped_output_dir = f"{output_dir}/v-{version}-p{product}-s{step}"
        os.makedirs(grouped_output_dir, exist_ok=True)

        fig, ax = plt.subplots(figsize=(12, 6))

        # Get unique components and their corresponding durations for the boxplot
        components = sorted(group['COMPONENT'].unique(), key=lambda x: (x.startswith('blazegraph'), x.startswith('quaque-flat'), x.startswith('quaque-condensed')), reverse=True)
        data_to_plot = [group[group['COMPONENT'] == comp]['DURATION (ms)'] for comp in components]

        # Create the boxplot
        bp = ax.boxplot(data_to_plot, patch_artist=True, tick_labels=components) # Added labels

        # Add colors to boxes for better distinction
        colors = plt.cm.viridis_r(np.linspace(0, 1, len(components)))
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)

        # Add median lines color
        for median in bp['medians']:
            median.set(color='red', linewidth=2)

        # Improve layout and labels
        ax.set_title(f'Duration Distribution\nVersion={version}, Product={product}, Step={step}, Query={query}')
        ax.set_ylabel('Duration (ms)')
        ax.set_xlabel('Component')
        # ax.tick_params(axis='x', rotation=45) # Rotate x-axis labels if they overlap
        ax.grid(True, linestyle='--', alpha=0.6) # Add grid lines
        
        # --- Create a safe filename for the plot ---
        safe_query = sanitize_filename(query)
        filepath = os.path.join(grouped_output_dir, f"whisker_duration_{safe_query}.png")
       
        plt.savefig(filepath, dpi=300)
        plt.close(fig)
        
def create_version_ratio_plot(data, limit=None):
    import pandas as pd
    import matplotlib.pyplot as plt

    # Read the uploaded JSON file into a pandas DataFrame
    # Use the file handle provided by the environment
    df = pd.DataFrame(data)
    if limit is not None:
        df = df[df['TRY'] >= limit]
    output_dir = 'plots/ratio'
    os.makedirs(output_dir, exist_ok=True)

    # Define the columns that identify a unique configuration
    config_cols = ['STEP', 'PRODUCT', 'QUERY', 'COMPONENT_NAME']

    # --- Step 1: Calculate total duration for each version within each configuration ---
    duration_per_version = df.groupby(config_cols + ['VERSION'])['DURATION (ms)'].sum().reset_index()

    # --- Step 2: Calculate total duration for each configuration (across all versions) ---
    total_duration_per_config = df.groupby(config_cols)['DURATION (ms)'].sum().reset_index()
    total_duration_per_config.rename(columns={'DURATION (ms)': 'TOTAL_DURATION_CONFIG'}, inplace=True)

    # --- Step 3: Merge the two results to calculate the ratio ---
    results_df = pd.merge(duration_per_version, total_duration_per_config, on=config_cols)

    # --- Step 4: Calculate the ratio ---
    results_df['RATIO'] = results_df.apply(
        lambda row: row['DURATION (ms)'] / row['TOTAL_DURATION_CONFIG'] if row['TOTAL_DURATION_CONFIG'] > 0 else 0,
        axis=1
    )

    # --- Step 5: Prepare for plotting ---
    # Get unique configurations
    unique_configs = results_df[config_cols].drop_duplicates().values.tolist()
    num_unique_configs = len(unique_configs)
    print(f"Found {num_unique_configs} unique configurations based on {config_cols}.")

    if num_unique_configs == 0:
        print("No data or configurations found to plot.")
    else:
        # associate step, product, query to list of components
        config_to_components = {}
        for config in unique_configs:
            step, product, query, component = config
            if (step, product, query) not in config_to_components:
                config_to_components[(step, product, query)] = []
            config_to_components[(step, product, query)].append(component)     
        
        # --- Step 6: Generate plots ---
        for (step, product, query), components in config_to_components.items():
            fig, ax = plt.subplots(figsize=(12, 6))
            
            components = sorted(components, key=lambda x: (x.startswith('blazegraph'), x.startswith('quaque-flat'), x.startswith('quaque-condensed')), reverse=True)

            # Filter data for the current configuration
            for component in components:
                # Filter the DataFrame for the current configuration
                config_cols = ['STEP', 'PRODUCT', 'QUERY', 'COMPONENT_NAME']
                # Create a tuple for the current configuration
                config = [step, product, query, component]
                config_filter = (results_df[config_cols] == pd.Series(config, index=config_cols)).all(axis=1)
                plot_data = results_df[config_filter].sort_values(by='VERSION')

                # Create plot
                ax.plot(plot_data['VERSION'], plot_data['RATIO'], marker='o', linestyle='-', label=component)

            # Set title and labels
            # Create a multi-line title for better readability
            title_str = f"Step: {step}, Prod: {product}\nQuery: {query}"
            ax.set_title(title_str, fontsize=9)
            ax.set_xlabel("Version")
            ax.set_ylabel("Time Ratio")
            ax.grid(True)
            ax.legend(title='Component', loc='upper left')  # Add legend to the plot
            
            # Ensure x-axis ticks are integers if versions are integers
            ax.xaxis.get_major_locator().set_params(integer=True)

            # --- Create a safe filename for the plot ---
            os.makedirs(f"{output_dir}/{step}_{product}", exist_ok=True)
            filepath = f"{output_dir}/{step}_{product}/duration_ratio_{query}.png"
            plt.savefig(filepath, dpi=300)
            plt.close(fig)

def sanitize_filename(name):
    """Removes or replaces characters invalid for filenames."""
    # Remove invalid characters
    name = re.sub(r'[<>:"/\\|?*]', '_', str(name))
    # Replace spaces with underscores
    name = name.replace(' ', '_')
    # Limit length if necessary (optional)
    max_len = 100
    if len(name) > max_len:
        name = name[:max_len]
    return name

if __name__ == "__main__":
    # Afficher les informations extraites

    log_file_path = os.getenv("LOG_FILE_PATH")
    if not log_file_path:
        raise EnvironmentError(
            "LOG_FILE_PATH environment variable is not set.")

    log_data = extract_log_info(log_file_path)

    whisker_duration_per_component_query_config(log_data, limit=50)
    create_version_ratio_plot(log_data, limit=50)
