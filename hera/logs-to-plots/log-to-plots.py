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
        components = sorted(group['COMPONENT'].unique(), key=lambda x: (x.startswith(
            'blazegraph'), x.startswith('quaque-flat'), x.startswith('quaque-condensed')), reverse=True)
        data_to_plot = [group[group['COMPONENT'] == comp]
                        ['DURATION (ms)'] for comp in components]

        # Create the boxplot
        bp = ax.boxplot(data_to_plot, patch_artist=True,
                        tick_labels=components)  # Added labels

        # Add colors to boxes for better distinction
        for patch, comp in zip(bp['boxes'], components):
            if comp.startswith('blazegraph'):
                patch.set_facecolor('blue')
            elif comp.startswith('quaque-flat'):
                patch.set_facecolor('orange')
            else:
                patch.set_facecolor('green')

        # Add median lines color
        for median in bp['medians']:
            median.set(color='red', linewidth=2)

        # Improve layout and labels
        ax.set_title(
            f'Duration Distribution\nVersion={version}, Product={product}, Step={step}, Query={query}')
        ax.set_ylabel('Duration (ms)')
        ax.set_xlabel('Component')
        # ax.tick_params(axis='x', rotation=45) # Rotate x-axis labels if they overlap
        ax.grid(True, linestyle='--', alpha=0.6)  # Add grid lines

        # --- Create a safe filename for the plot ---
        safe_query = sanitize_filename(query)
        filepath = os.path.join(
            grouped_output_dir, f"whisker_duration_{safe_query}.png")

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
    duration_per_version = df.groupby(
        config_cols + ['VERSION'])['DURATION (ms)'].sum().reset_index()

    # --- Step 2: Calculate total duration for each configuration (across all versions) ---
    total_duration_per_config = df.groupby(
        config_cols)['DURATION (ms)'].sum().reset_index()
    total_duration_per_config.rename(
        columns={'DURATION (ms)': 'TOTAL_DURATION_CONFIG'}, inplace=True)

    # --- Step 3: Merge the two results to calculate the ratio ---
    results_df = pd.merge(duration_per_version,
                          total_duration_per_config, on=config_cols)

    # --- Step 4: Calculate the ratio ---
    results_df['RATIO'] = results_df.apply(
        lambda row: row['DURATION (ms)'] /
        row['TOTAL_DURATION_CONFIG'] if row['TOTAL_DURATION_CONFIG'] > 0 else 0,
        axis=1
    )

    # --- Step 5: Prepare for plotting ---
    # Get unique configurations
    unique_configs = results_df[config_cols].drop_duplicates().values.tolist()
    num_unique_configs = len(unique_configs)
    print(
        f"Found {num_unique_configs} unique configurations based on {config_cols}.")

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

            components = sorted(
                components,
                key=lambda x:
                    (x.startswith('blazegraph'), x.startswith(
                        'quaque-flat'), x.startswith('quaque-condensed')),
                    reverse=True
            )

            # Filter data for the current configuration
            for component in components:
                # Filter the DataFrame for the current configuration
                config_cols = ['STEP', 'PRODUCT', 'QUERY', 'COMPONENT_NAME']
                # Create a tuple for the current configuration
                config = [step, product, query, component]
                config_filter = (results_df[config_cols] == pd.Series(
                    config, index=config_cols)).all(axis=1)
                plot_data = results_df[config_filter].sort_values(by='VERSION')

                # Assign color based on component name
                if component.startswith('blazegraph'):
                    color = 'blue'
                elif component.startswith('quaque-flat'):
                    color = 'orange'
                else:
                    color = 'green'

                ax.plot(plot_data['VERSION'], plot_data['RATIO'],
                        marker='o', linestyle='-', label=component, color=color)

            # Set title and labels
            # Create a multi-line title for better readability
            title_str = f"Step: {step}, Prod: {product}\nQuery: {query}"
            ax.set_title(title_str, fontsize=9)
            ax.set_xlabel("Version")
            ax.set_ylabel("Time Ratio")
            ax.grid(True)
            # Add legend to the plot
            ax.legend(title='Component', loc='upper left')

            # Ensure x-axis ticks are integers if versions are integers
            ax.xaxis.get_major_locator().set_params(integer=True)

            # --- Create a safe filename for the plot ---
            os.makedirs(f"{output_dir}/{step}_{product}", exist_ok=True)
            filepath = f"{output_dir}/{step}_{product}/duration_ratio_{query}.png"
            plt.savefig(filepath, dpi=300)
            plt.close(fig)


def create_version_normalized_duration_plot(data, limit=None):
    """
    Generates plots showing normalized execution time per version for configurations.

    Each plot corresponds to a (STEP, PRODUCT, QUERY) combination and shows
    lines for different COMPONENT_NAMEs. The y-axis represents the duration
    of a specific version divided by the duration of the lowest version number
    found within that specific (STEP, PRODUCT, QUERY, COMPONENT_NAME) group.

    Args:
        data (list or similar): Input data convertible to a pandas DataFrame.
                                 Expected columns: 'STEP', 'PRODUCT', 'QUERY',
                                 'COMPONENT_NAME', 'VERSION', 'DURATION (ms)', 'TRY'.
        limit (int, optional): If provided, only include rows where 'TRY' >= limit.
                               Defaults to None.
    """
    import pandas as pd
    import matplotlib.pyplot as plt
    import os  # Import os module

    # --- Data Loading and Initial Filtering ---
    df = pd.DataFrame(data)
    if limit is not None:
        df = df[df['TRY'] >= limit]

    # Ensure essential columns exist
    config_cols = ['STEP', 'PRODUCT', 'QUERY', 'COMPONENT_NAME']
    required_cols = config_cols + ['VERSION', 'DURATION (ms)']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        print(f"Error: Missing required columns: {missing}")
        return  # Exit if essential columns are missing

    output_dir = 'plots/normalized_duration'  # Changed output directory name
    os.makedirs(output_dir, exist_ok=True)

    # --- Step 1: Calculate sum of durations for each version within each config ---
    duration_per_version = df.groupby(
        config_cols + ['VERSION']
    )['DURATION (ms)'].sum().reset_index()
    duration_per_version.rename(
        columns={'DURATION (ms)': 'SUM_DURATION_MS'}, inplace=True)

    if duration_per_version.empty:
        print("No data remaining after grouping. Cannot generate plots.")
        return

    # --- Step 2: Find the duration of the lowest version for each config ---
    # Get the index of the row with the minimum version for each group
    min_version_indices = duration_per_version.loc[duration_per_version.groupby(
        config_cols)['VERSION'].idxmin()]
    # Select relevant columns and rename the duration column to represent the normalization factor
    normalization_factors = min_version_indices[config_cols + [
        'SUM_DURATION_MS']].copy()
    normalization_factors.rename(
        columns={'SUM_DURATION_MS': 'LOWEST_VERSION_DURATION'}, inplace=True)

    # --- Step 3: Merge normalization factor back to the main data ---
    results_df = pd.merge(duration_per_version,
                          normalization_factors, on=config_cols, how='left')

    # --- Step 4: Calculate the Normalized Duration ---
    results_df['NORMALIZED_DURATION'] = results_df.apply(
        lambda row: row['SUM_DURATION_MS'] / row['LOWEST_VERSION_DURATION']
        if pd.notna(row['LOWEST_VERSION_DURATION']) and row['LOWEST_VERSION_DURATION'] > 0
        else 0,  # Handle cases where merge failed or duration is 0
        axis=1
    )

    # --- Step 5: Prepare for plotting (Group by Step, Product, Query) ---
    unique_configs_for_plotting = results_df[[
        'STEP', 'PRODUCT', 'QUERY']].drop_duplicates().values.tolist()
    num_plot_configs = len(unique_configs_for_plotting)
    print(
        f"Found {num_plot_configs} unique (STEP, PRODUCT, QUERY) combinations for plotting.")

    if num_plot_configs == 0:
        print("No configurations found to plot.")
    else:
        # --- Step 6: Generate plots ---
        for plot_config in unique_configs_for_plotting:
            step, product, query = plot_config
            fig, ax = plt.subplots(figsize=(12, 6))

            # Filter results_df for the current Step, Product, Query
            plot_group_data = results_df[
                (results_df['STEP'] == step) &
                (results_df['PRODUCT'] == product) &
                (results_df['QUERY'] == query)
            ].copy()

            # Get components for this group and sort them for consistent plotting order
            components = sorted(
                plot_group_data['COMPONENT_NAME'].unique(),
                key=lambda x:
                (not x.startswith('blazegraph'), not x.startswith(
                    'quaque-flat'), not x.startswith('quaque-condensed'))
            )

            # Plot data for each component in the current group
            for component in components:
                # Filter the group data for the current component
                component_data = plot_group_data[plot_group_data['COMPONENT_NAME'] == component].sort_values(
                    by='VERSION')

                if component_data.empty:
                    continue  # Skip if no data for this component in the group

                # Assign color based on component name (same logic as before)
                if component.startswith('blazegraph'):
                    color = 'blue'
                elif component.startswith('quaque-flat'):
                    color = 'orange'
                # Added condition for condensed
                elif component.startswith('quaque-condensed'):
                    color = 'green'
                else:  # Default color for other components
                    color = 'red'

                # Plot NORMALIZED_DURATION vs VERSION
                ax.plot(component_data['VERSION'], component_data['NORMALIZED_DURATION'],
                        marker='o', linestyle='-', label=component, color=color)

            # --- Plot Customization ---
            title_str = f"Step: {step}, Product: {product}\nQuery: {query}"
            ax.set_title(title_str, fontsize=11)  # Adjusted font size slightly
            ax.set_xlabel("Version")
            # Updated Y-axis label
            ax.set_ylabel(
                "Normalized Duration (Time / Time of Lowest Version)")
            ax.grid(True)

            # Add legend only if there are labels to show
            if ax.get_legend_handles_labels()[0]:
                # Changed location to 'best'
                ax.legend(title='Component', loc='best')

            # Ensure x-axis ticks are integers
            try:  # Added try-except for robustness
                ax.xaxis.get_major_locator().set_params(integer=True)
            except AttributeError:
                print(
                    f"Warning: Could not set integer ticks for x-axis on plot: {title_str}")

            # Set y-axis minimum to 0 for better interpretation, maybe add some padding
            ax.set_ylim(bottom=0)

            # --- Saving the plot ---
            # Define subdirectory
            step_prod_dir = f"{output_dir}/{step}_{product}"
            # Create subdirectory if needed
            os.makedirs(step_prod_dir, exist_ok=True)
            # Sanitize query part of filename (replace non-alphanumeric with underscore)
            safe_query = "".join(c if c.isalnum() else "_" for c in str(query))
            filepath = f"{step_prod_dir}/normalized_duration_{safe_query}.png"

            plt.savefig(filepath, dpi=300)  # Use bbox_inches
            plt.close(fig)  # Close figure to free memory


def sanitize_filename(name, max_len=100):
    """Removes or replaces characters invalid for filenames."""
    # Remove invalid characters
    name = re.sub(r'[<>:"/\\|?*]', '_', str(name))
    # Replace spaces with underscores
    name = name.replace(' ', '_')
    # Limit length if necessary (optional)
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
    create_version_normalized_duration_plot(log_data, limit=50)
    create_version_ratio_plot(log_data, limit=50)
