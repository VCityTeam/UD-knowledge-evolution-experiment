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

    parts = component.split('-')
    component_parts = []
    parts = parts[3:]

    for part in parts:
        # Check if the part consists only of digits
        if part.isdigit():
            # Stop collecting parts once a numeric part is found
            break
        # Add the non-numeric part to our list
        component_parts.append(part)

    return '-'.join(component_parts)


def extract_log_info(log_file_path: str, queries_info: str, min_count_version: int, min_count_component: int, min_repeat: int):
    # Définir une expression régulière pour correspondre au format du log
    log_pattern = r'\{"component":"(?P<component>[^"]+)","query":"(?P<query>[^"]+)","try":"(?P<try>[^"]+)","duration":"(?P<duration>[^"]+)","version":"(?P<version>[^"]+)","product":"(?P<product>[^"]+)","step":"(?P<step>[^"]+)","time":"(?P<time>[^"]+)"\}'
    extracted_data = []
    
    with open(queries_info, 'r') as f:
        queries_configuration = json.load(f)

    # Lire le fichier de logs
    with open(log_file_path, 'r') as file:
        for line in file:
            # Chercher les correspondances avec le pattern
            match = re.search(log_pattern, line)
            if match:
                query_name = f"query-{match.group('query').split('-')[-1].split('.')[0]}"
                query_info = queries_configuration.get(query_name)

                # Extraire COMPONENT, DURATION, et FILE
                component = match.group('component')
                # keep only the number of the query
                query = query_name
                nb_try = int(match.group('try'))
                version_conf = int(match.group('version'))
                step_conf = int(match.group('step'))
                # Convertir le temps en millisecondes
                duration_ms = int(match.group('duration').replace("ms", ""))
                time_unix = int(match.group('time'))

                extracted_data.append({
                    "VERSION": version_conf,
                    "STEP": step_conf,
                    "COMPONENT": component,
                    "DURATION (ms)": duration_ms,
                    "QUERY": query,
                    "TRY": nb_try,
                    "TIME": time_unix,
                    "COMPONENT_NAME": get_component_name(component),
                    "AGGREGATIVE": query_info["aggregative"] if query_info else None
                })

    extracted_data = remove_all_with_less_than_repeat(data=extracted_data, repeat=min_repeat)
    print(f"After remove_all_with_less_than_repeat: {len(extracted_data)}")
    extracted_data = remove_all_with_less_than_count_version(data=extracted_data, count=min_count_version)
    print(f"After remove_all_with_less_than_count_version: {len(extracted_data)}")
    extracted_data = remove_all_with_less_than_count_component(data=extracted_data, count=min_count_component)
    print(f"After remove_all_with_less_than_count_component: {len(extracted_data)}")

    return extracted_data

def remove_all_with_less_than_repeat(data, repeat=200):
    """
    Group by VERSION, STEP, QUERY and
    remove all entries from data when the max TRY is less than repeat
    """
    import pandas as pd
    df = pd.DataFrame(data)
    
    max_lower_than_repeat = []
    for name, group in df.groupby(['VERSION', 'STEP', 'QUERY']):
        max_try = group['TRY'].max()
        if max_try < repeat:
            max_lower_than_repeat.append(name)

    # Remove all entries from data have the same VERSION, STEP, QUERY in max_lower_than_repeat
    for name in max_lower_than_repeat:
        version, step, query = name
        df = df[~((df['VERSION'] == version) & (df['STEP'] == step) & (df['QUERY'] == query))]

    return df.to_dict(orient='records')

def remove_all_with_less_than_count_version(data, count=4):
    """
    Group by STEP, QUERY, COMPONENT and
    remove all entries from data when the count of version is less than count
    """
    import pandas as pd
    df = pd.DataFrame(data)
    
    # Count the number of unique versions for each group
    version_counts = df.groupby(['STEP', 'QUERY', 'COMPONENT_NAME'])['VERSION'].nunique().reset_index()
    version_counts.rename(columns={'VERSION': 'COUNT_VERSION'}, inplace=True)

    # Filter groups with less than count unique versions
    groups_to_remove = version_counts[version_counts['COUNT_VERSION'] < count]

    # Remove all entries from data have the same STEP, QUERY in groups_to_remove
    for _, row in groups_to_remove.iterrows():
        step, query, _, _ = row
        df = df[~((df['STEP'] == step) & (df['QUERY'] == query))]

    return df.to_dict(orient='records')

def remove_all_with_less_than_count_component(data, count=3):
    """
    Group by STEP, QUERY, VERSION and
    remove all entries from data when the count of component is less than count
    """
    import pandas as pd
    df = pd.DataFrame(data)
    
    # Count the number of unique components for each group
    component_counts = df.groupby(['STEP', 'QUERY', 'VERSION'])['COMPONENT_NAME'].nunique().reset_index()
    component_counts.rename(columns={'COMPONENT_NAME': 'COUNT_COMPONENT'}, inplace=True)

    # Filter groups with less than count unique components
    groups_to_remove = component_counts[component_counts['COUNT_COMPONENT'] < count]

    # Remove all entries from data have the same STEP, QUERY in groups_to_remove
    for _, row in groups_to_remove.iterrows():
        step, query, version, _ = row
        df = df[~((df['STEP'] == step) & (df['QUERY'] == query) & (df['VERSION'] == version))]

    return df.to_dict(orient='records')

def whisker_duration_per_component_query_config(data, scale="linear", limit=None):
    import pandas as pd
    import matplotlib.pyplot as plt
    import os

    print("Starting to create boxplots for duration per component and query configuration.")

    grouping_cols = ['VERSION', 'STEP', 'QUERY']
    df = pd.DataFrame(data)
    if limit is not None:
        df = df[df['TRY'] >= limit]
    output_dir = 'plots/whiskers'
    os.makedirs(output_dir, exist_ok=True)
    grouped_data = df.groupby(grouping_cols)
    print(f"Found {len(grouped_data)} groups based on {grouping_cols}.")

    for name, group in grouped_data:
        # Extract group keys
        version, step, query = name
        grouped_output_dir = f"{output_dir}/{scale}/v-{version}-s{step}"
        os.makedirs(grouped_output_dir, exist_ok=True)

        fig, ax = plt.subplots(figsize=(12, 6))

        # Get unique components and their corresponding durations for the boxplot
        components = sorted(group['COMPONENT_NAME'].unique(), key=lambda x: (x.startswith(
            'blazegraph'), x.startswith('jena'), x.startswith('quaque-flat'), x.startswith('quaque-condensed')), reverse=True)
        data_to_plot = [group[group['COMPONENT_NAME'] == comp]
                        ['DURATION (ms)'] for comp in components]

        # Create the boxplot
        bp = ax.boxplot(data_to_plot, patch_artist=True,
                        tick_labels=components)  # Added labels

        # Add colors to boxes for better distinction
        for patch, comp in zip(bp['boxes'], components):
            if comp.startswith('blazegraph'):
                patch.set_facecolor('blue')
            elif comp.startswith('jena'):
                patch.set_facecolor('purple')
            elif comp.startswith('quaque-flat'):
                patch.set_facecolor('orange')
            else:
                patch.set_facecolor('green')

        # Add median lines color
        for median in bp['medians']:
            median.set(color='red', linewidth=2)

        # Improve layout and labels
        ax.set_title(
            f'Duration Distribution\nVersion={version}, Step={step}, Query={query}')
        ax.set_ylabel('Duration (ms)')
        ax.set_xlabel('Component')

        # Set y-axis scale if requested
        if scale == "log":
            ax.set_yscale("log")
            ax.set_ylabel('Duration Log(ms)')

        # ax.tick_params(axis='x', rotation=45) # Rotate x-axis labels if they overlap
        ax.grid(True, linestyle='--', alpha=0.6)  # Add grid lines

        # --- Create a safe filename for the plot ---
        safe_query = sanitize_filename(query)
        filepath = os.path.join(
            grouped_output_dir, f"whisker_duration_{safe_query}.png")

        plt.savefig(filepath, dpi=300)
        plt.close(fig)


def create_duration_median_plot(data, scale="linear", limit=None):
    import pandas as pd
    import matplotlib.pyplot as plt
    
    print("Starting to create median duration plots.")

    # Read the uploaded JSON file into a pandas DataFrame
    # Use the file handle provided by the environment
    df = pd.DataFrame(data)
    if limit is not None:
        df = df[df['TRY'] >= limit]
    output_dir = f'plots/median_duration/{scale}'
    os.makedirs(output_dir, exist_ok=True)

    # Define the columns that identify a unique configuration
    config_cols = ['STEP', 'QUERY', 'COMPONENT_NAME']

    # --- Step 1: Calculate median duration for each version within each configuration ---
    median_duration_per_version = df.groupby(
        config_cols + ['VERSION'])['DURATION (ms)'].median().reset_index()
    median_duration_per_version.rename(
        columns={'DURATION (ms)': 'MEDIAN_DURATION_CONFIG'}, inplace=True)

    # --- Step 2: Prepare for plotting ---
    # Get unique configurations
    unique_configs = median_duration_per_version[config_cols].drop_duplicates().values.tolist()
    num_unique_configs = len(unique_configs)
    print(
        f"Found {num_unique_configs} unique configurations based on {config_cols}.")

    if num_unique_configs == 0:
        print("No data or configurations found to plot.")
    else:
        # associate step, query to list of components
        config_to_components = {}
        for config in unique_configs:
            step, query, component = config
            if (step, query) not in config_to_components:
                config_to_components[(step, query)] = []
            config_to_components[(step, query)].append(component)

        # --- Step 3: Generate plots ---
        for (step, query), components in config_to_components.items():
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
                config_cols = ['STEP', 'QUERY', 'COMPONENT_NAME']
                # Create a tuple for the current configuration
                config = [step, query, component]
                config_filter = (median_duration_per_version[config_cols] == pd.Series(
                    config, index=config_cols)).all(axis=1)
                plot_data = median_duration_per_version[config_filter].sort_values(by='VERSION')

                # Assign color based on component name
                if component.startswith('blazegraph'):
                    color = 'blue'
                elif component.startswith('jena'):
                    color = 'purple'
                elif component.startswith('quaque-flat'):
                    color = 'orange'
                else:
                    color = 'green'

                ax.plot(plot_data['VERSION'], plot_data['MEDIAN_DURATION_CONFIG'],
                        marker='o', linestyle='-', label=component, color=color)

            # Set title and labels
            # Create a multi-line title for better readability
            title_str = f"Step: {step}, Query: {query}"
            ax.set_title(title_str, fontsize=9)
            ax.set_xlabel("Version")
            ax.set_ylabel("Median Duration (ms)")
            if scale == "log":
                ax.set_yscale("log")
                ax.set_ylabel("Median Duration Log(ms)")

            ax.grid(True)
            # Add legend to the plot
            ax.legend(title='Component', loc='upper left')

            # Ensure x-axis ticks are integers if versions are integers
            ax.xaxis.get_major_locator().set_params(integer=True)

            # --- Create a safe filename for the plot ---
            os.makedirs(f"{output_dir}/{step}", exist_ok=True)
            filepath = f"{output_dir}/{step}/duration_median_{query}.png"
            plt.savefig(filepath, dpi=300)
            plt.close(fig)


def create_version_normalized_duration_plot(data, limit=None):
    """
    Generates plots showing normalized execution time per version for configurations.

    Each plot corresponds to a (STEP, QUERY) combination and shows
    lines for different COMPONENT_NAMEs. The y-axis represents the duration
    of a specific version divided by the duration of the lowest version number
    found within that specific (STEP, QUERY, COMPONENT_NAME) group.

    Args:
        data (list or similar): Input data convertible to a pandas DataFrame.
                                 Expected columns: 'STEP', 'QUERY',
                                 'COMPONENT_NAME', 'VERSION', 'DURATION (ms)', 'TRY'.
        limit (int, optional): If provided, only include rows where 'TRY' >= limit.
                               Defaults to None.
    """
    import pandas as pd
    import matplotlib.pyplot as plt
    import os  # Import os module
    
    print("Starting to create version normalized duration plots.")

    # --- Data Loading and Initial Filtering ---
    df = pd.DataFrame(data)
    if limit is not None:
        df = df[df['TRY'] >= limit]

    # Ensure essential columns exist
    config_cols = ['STEP', 'QUERY', 'COMPONENT_NAME']
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

    # --- Step 5: Prepare for plotting (Group by Step, Query) ---
    unique_configs_for_plotting = results_df[[
        'STEP', 'QUERY']].drop_duplicates().values.tolist()
    num_plot_configs = len(unique_configs_for_plotting)
    print(
        f"Found {num_plot_configs} unique (STEP, QUERY) combinations for plotting.")

    if num_plot_configs == 0:
        print("No configurations found to plot.")
    else:
        # --- Step 6: Generate plots ---
        for plot_config in unique_configs_for_plotting:
            step, query = plot_config
            fig, ax = plt.subplots(figsize=(12, 6))

            # Filter results_df for the current Step, Query
            plot_group_data = results_df[
                (results_df['STEP'] == step) &
                (results_df['QUERY'] == query)
            ].copy()

            # Get components for this group and sort them for consistent plotting order
            components = sorted(
                plot_group_data['COMPONENT_NAME'].unique(),
                key=lambda x:
                (not x.startswith('blazegraph'), not x.startswith('jena'), not x.startswith(
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
                elif component.startswith('jena'):
                    color = 'purple'
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
            title_str = f"Step: {step}: Query: {query}"
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
            step_prod_dir = f"{output_dir}/{step}"
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

def store_data_to_json(data, file_path):
    """
    Store the data to a json file
    Args:
        data (list): List of dictionaries
        file_path (str): Path to the json file
    """
    with open(file_path, 'w') as f:
        json.dump(data, f)
        
def check_shapiro_wilk_test(data: list, warmup: int):
    import pandas as pd
    from scipy import stats
    
    print("The Shapiro-Wilk test is a statistical test used to determine whether a sample comes from a normally distributed population.")   
    print("This means that the data does not significantly deviate from a normal distribution.") 

    df = pd.DataFrame(data)
    
    results = []
    
    # Remove warmup tries
    df = df[df['TRY'] > warmup]
    
    grouped = df.groupby(['STEP', 'QUERY', 'COMPONENT_NAME', 'VERSION'])

    for name, group in grouped:
        step, query, component, version = name
        if len(group) < 2:
            print(f"Not enough data for statistical test for {step, query, component, version}")
            continue
        # Perform Shapiro-Wilk test for normality
        grouped_data = group['DURATION (ms)']
        stat, p_value = stats.shapiro(grouped_data)
        alpha = 0.05
        if p_value > alpha:
            print(f"Data for {step, query, component, version} is normally distributed (p={p_value:.3f})")
            
        results.append({
            'STEP': int(step),
            'QUERY': query,
            'VERSION': int(version),
            'COMPONENT_NAME': component,
            'W_STATISTIC': stat,
            'P_VALUE': p_value,
            'NORMALLY_DISTRIBUTED': str(p_value > alpha),
            'MEAN': grouped_data.mean(),
            'MEDIAN': grouped_data.median(),
            '75TH_PERCENTILE': grouped_data.quantile(0.75),
            '95TH_PERCENTILE': grouped_data.quantile(0.95),
        })
        
    # save results to a json file
    with open('shapiro_wilk_test_results.json', 'w') as f:
        json.dump(results, f, indent=4)
        
    return results
            
def check_Mann_Whitney_U_test(data: list, warmup: int):
    import pandas as pd
    from scipy import stats
    from itertools import combinations
    
    print("Mann-Whitney U test is a non-parametric test used to determine whether there is a significant difference between the distributions of two independent samples.")
    print("No significant difference between comp1 and comp2 for the given step, query, and version, meaning the statistical test did not find strong evidence that the two are different.")
    print("Significant difference between comp1 and comp2 for the given step, query, and version, meaning the statistical test found strong evidence that the two are different.")

    df = pd.DataFrame(data)
    
    results = []
    
    # Remove warmup tries
    df = df[df['TRY'] > warmup]
    
    grouped = df.groupby(['STEP', 'QUERY', 'VERSION'])

    for name, group in grouped:
        step, query, version = name
        components = group['COMPONENT_NAME'].unique()
        if len(components) < 2:
            print(f"Not enough components for statistical test for {step, query, version}")
            continue
        
        for comp1, comp2 in combinations(components, 2):
            data1 = group[group['COMPONENT_NAME'] == comp1]['DURATION (ms)']
            data2 = group[group['COMPONENT_NAME'] == comp2]['DURATION (ms)']
            if len(data1) < 2 or len(data2) < 2:
                print(f"Not enough data for Mann-Whitney U test between {comp1} and {comp2} for {step, query, version}")
                continue
            stat, p_value = stats.mannwhitneyu(data1, data2, alternative='two-sided')
            
            alpha = 0.05
            if p_value <= alpha:
                print(f"Significant difference between {comp1} and {comp2} for {step, query, version} (p={p_value:.3f})")
                
            results.append({
                'STEP': int(step),
                'QUERY': query,
                'VERSION': int(version),
                'COMPONENT_1': comp1,
                'COMPONENT_2': comp2,
                'U_STATISTIC': stat,
                'P_VALUE': p_value,
                'SIGNIFICANT': str(p_value <= alpha)
            })

    # save results to a json file
    with open('mann_whitney_u_test_results.json', 'w') as f:
        json.dump(results, f, indent=4)
        
    return results

def create_shapiro_wilk_test_table(results: list):
    import pandas as pd
    
    filename = 'shapiro_wilk_test_results.csv'
    
    df = pd.DataFrame(results)
    
    # Create a pivot table as before
    df = df.pivot_table(index=['STEP', 'QUERY', 'VERSION'],
                        columns='COMPONENT_NAME',
                        values=['MEAN', 'MEDIAN', '75TH_PERCENTILE', '95TH_PERCENTILE'],
                        aggfunc='first')

    # Prepare multi-level columns: first row is component, second row is statistic
    df.columns = pd.MultiIndex.from_tuples([(comp, stat) for stat, comp in df.columns])

    # Sort columns by component_name (first level)
    df = df.sort_index(axis=1, level=0)

    # Reset index for saving to CSV
    df.reset_index(inplace=True)

    # Save with multi-level columns
    df.to_csv(filename, index=False, index_label=['STEP', 'QUERY', 'VERSION'], header=True)
    
    print("Shapiro-Wilk test results saved to shapiro_wilk_test_results.csv")
    
    return filename

def highlight_and_bold_quaque(row):
    import pandas as pd
    
    # Find all 'quaque-condensed' columns
    quaque_condensed_cols = [col for col in row.index if col[0].startswith('quaque-condensed')]
    # Find all 'quaque-flat' columns
    quaque_flat_cols = [col for col in row.index if col[0].startswith('quaque-flat')]
    # Find all 'jena' columns
    jena_cols = [col for col in row.index if col[0].startswith('jena')]
    # Find all 'blazegraph' columns
    blazegraph_cols = [col for col in row.index if col[0].startswith('blazegraph')]
    # Find all other component columns
    not_quaque_condensed_cols = [col for col in row.index if not col[0].startswith('quaque-condensed')]
    
    valid_rows = []

    styles = [''] * len(row)
    # For each statistic, check if any quaque value is lower than all others (for MEAN, MEDIAN, etc.)
    for stat in ['MEAN', 'MEDIAN', '75TH_PERCENTILE', '95TH_PERCENTILE']:
        quaque_condensed_stat_cols = [col for col in quaque_condensed_cols if col[1] == stat]
        quaque_flat_stat_cols = [col for col in quaque_flat_cols if col[1] == stat]
        jena_stat_cols = [col for col in jena_cols if col[1] == stat]
        blazegraph_stat_cols = [col for col in blazegraph_cols if col[1] == stat]
        other_stat_cols = [col for col in not_quaque_condensed_cols if col[1] == stat]
        
        for b_col in blazegraph_stat_cols:
            other_vals = [row[o_col] for o_col in other_stat_cols if pd.notnull(row[o_col])]
            idx = row.index.get_loc(b_col)
            styles[idx] = 'background-color: #B3D8F8' # Light blue
        
        for j_col in jena_stat_cols:
            other_vals = [row[o_col] for o_col in other_stat_cols if pd.notnull(row[o_col])]
            idx = row.index.get_loc(j_col)
            styles[idx] = 'background-color: #E6E6FA' # Light purple
        
        for q_col in quaque_flat_stat_cols:
            other_vals = [row[o_col] for o_col in other_stat_cols if pd.notnull(row[o_col])]
            idx = row.index.get_loc(q_col)
            styles[idx] = 'background-color: #FFD580' # Light orange
        
        for q_col in quaque_condensed_stat_cols:
            q_val = row[q_col]
            idx = row.index.get_loc(q_col)
            styles[idx] = 'background-color: #90ee90' # Light green
            if q_col[1] == 'MEDIAN':
                other_vals = [row[o_col] for o_col in other_stat_cols if pd.notnull(row[o_col])]
                if all(q_val < o_val for o_val in other_vals if pd.notnull(o_val)):
                    valid_rows.append(row.name)
                    
    # Bold the lines where quaque-condensed is the best in MEDIAN
    # Make the row more visible
    for i in valid_rows:
        for j in range(len(styles)):
            if styles[j] != '':
                styles[j] += '; font-weight: bold; border: 2px solid black'
            else:
                styles[j] = 'font-weight: bold; border: 2px solid black'

    return styles

def highlight_quaque_csv(filename: str):
    import pandas as pd

    df = pd.read_csv(filename, header=[0, 1])
    styled_df = df.style.apply(highlight_and_bold_quaque, axis=1)

    styled_filename = filename.replace('.csv', '_highlighted.html')
    styled_df.to_html(styled_filename)

    print(f"Highlighted table saved to {styled_filename}")

if __name__ == "__main__":
    # Afficher les informations extraites
    min_repeat = int(os.getenv("COUNT_REPEAT", 200))
    min_count_version = int(os.getenv("COUNT_VERSION", 3))
    min_count_component = int(os.getenv("COUNT_COMPONENT", 3))
    mode = os.getenv("MODE", "stats")

    log_file_path = os.getenv("LOG_FILE_PATH")
    if not log_file_path:
        raise EnvironmentError(
            "LOG_FILE_PATH environment variable is not set.")
        
    queries_info = os.getenv("QUERIES_CONFIGURATION", "time-logs-to-plots/queries_configuration.json")

    print(f"Log file path: {log_file_path}")
    print(f"Minimum repeat: {min_repeat}")
    print(f"Minimum count version: {min_count_version}")
    print(f"Minimum count component: {min_count_component}")

    log_data = extract_log_info(log_file_path, queries_info, min_count_version, min_count_component, min_repeat)

    store_data_to_json(data=log_data, file_path="log_data.json")
    
    if mode == "plots" or mode == "all":
        for scale in ["linear", "log"]:
            whisker_duration_per_component_query_config(data=log_data, scale=scale, limit=50)
            create_duration_median_plot(data=log_data, scale=scale, limit=50)

        create_version_normalized_duration_plot(data=log_data, limit=50)
        
    if mode == "stats" or mode == "all":
        print("------------------- P-Value Test (Shapiro-Wilk) -------------------")
        shapiro_wilk_test_results = check_shapiro_wilk_test(data=log_data, warmup=20)
        print("------------------- Mann-Whitney U Test -------------------")
        mann_whitney_u_test_results = check_Mann_Whitney_U_test(data=log_data, warmup=20)
    
        print("Creating statistical test tables in CSV format.")
        filename = create_shapiro_wilk_test_table(shapiro_wilk_test_results)
        
        highlight_quaque_csv(filename)
