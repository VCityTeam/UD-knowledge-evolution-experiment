import json
import re
import os


# --- Utility Functions ---
def get_component_name(component: str):
    """
    Extracts the component name from a given string.
    The component name is the part after the last dot in the string.
    """
    if component.startswith("jena"):
        return "jena"
    
    if component.startswith("conver-g-flat"):
        return "conver-g-flat"
    
    if component.startswith("conver-g"):
        return "conver-g-condensed"
    
    return component


def extract_csv_info(csv_file_path: str, queries_info: str, min_repeat: int):
    """
    Extracts log information from a csv file and filters it based on repeat.
    """
    import pandas as pd
    
    extracted_data = []

    with open(queries_info, 'r') as f:
        queries_configuration = json.load(f)

    # Read the CSV file
    # Assuming comma separator as it is a CSV, but handling potential whitespace if needed
    try:
        df = pd.read_csv(csv_file_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return []

    for index, row in df.iterrows():
        # DATE	POLICY	GRANULARITY	TOOL	QUERY	RUN_ID	TIME_MS
        query_str = str(row['QUERY'])
        
        # Determine the key in configuration
        if query_str in queries_configuration:
            config_key = query_str
        elif f"{query_str}.rq" in queries_configuration:
            config_key = f"{query_str}.rq"
        else:
            config_key = query_str # Fallback, might result in None for query_info
            
        query_info = queries_configuration.get(config_key)
        
        component = row['TOOL']
        # Use the query string from CSV as the identifier, maybe strip .rq for cleaner plots if present
        query_id = query_str.replace(".rq", "")
        
        nb_try = int(row['RUN_ID'])
        duration_ms = int(row['TIME_MS'])
        
        extracted_data.append({
            "POLICY": row['POLICY'],
            "GRANULARITY": row['GRANULARITY'],
            "TOOL": component,
            "TIME_MS": duration_ms,
            "QUERY": query_id,
            "RUN_ID": nb_try,
            "TOOL_NAME": get_component_name(component),
            "AGGREGATIVE": query_info["aggregative"] if query_info else None
        })

    extracted_data = remove_all_with_less_than_repeat(extracted_data, min_repeat)
    print(f"After remove_all_with_less_than_repeat: {len(extracted_data)}")
    extracted_data = remove_all_with_granularity_not_day(extracted_data)
    print(f"After remove_all_with_granularity_not_day: {len(extracted_data)}")
    return extracted_data


def remove_all_with_less_than_repeat(data, repeat=200):
    """
    Remove entries where the max RUN_ID for (POLICY, GRANULARITY, QUERY) is less than repeat.
    """
    import pandas as pd
    df = pd.DataFrame(data)
    group_cols = ['POLICY', 'GRANULARITY', 'QUERY']
    
    # Ensure columns exist
    available_cols = [col for col in group_cols if col in df.columns]
    
    max_lower_than_repeat = []
    for name, group in df.groupby(available_cols):
        if group['RUN_ID'].max() < repeat:
            max_lower_than_repeat.append(name)
            
    # Filter out
    # This is a bit complex to do efficiently with a list of tuples and dynamic columns
    # So we'll use merge or isin
    
    # Create a set of signatures to remove
    signatures_to_remove = set(max_lower_than_repeat)
    
    def should_keep(row):
        sig = tuple(row[col] for col in available_cols)
        return sig not in signatures_to_remove

    if not df.empty:
        df = df[df.apply(should_keep, axis=1)]
        
    return df.to_dict(orient='records')


def remove_all_with_granularity_not_day(data):
    """
    Remove entries where GRANULARITY is not 'day'.
    """
    import pandas as pd
    df = pd.DataFrame(data)
    if 'GRANULARITY' in df.columns:
        df = df[df['GRANULARITY'] == 'day']
    return df.to_dict(orient='records')

def whisker_duration_per_component_query_config(data, scale="linear", limit=None):
    import pandas as pd
    import matplotlib.pyplot as plt
    import os

    print("Starting to create boxplots for duration per component and query configuration.")

    grouping_cols = ['POLICY', 'GRANULARITY', 'QUERY']
    df = pd.DataFrame(data)
    if limit is not None:
        df = df[df['RUN_ID'] >= limit]
    output_dir = 'plots/whiskers'
    os.makedirs(output_dir, exist_ok=True)
    
    # Ensure columns exist
    available_cols = [col for col in grouping_cols if col in df.columns]
    grouped_data = df.groupby(available_cols)
    print(f"Found {len(grouped_data)} groups based on {available_cols}.")

    for name, group in grouped_data:
        # Extract group keys
        # name is a tuple of values corresponding to available_cols
        if not isinstance(name, tuple):
            name = (name,)
            
        # We construct a path from it
        path_parts = [str(part) for part in name]
        # Last part is query, usually we want folders for the rest
        folder_parts = path_parts[:-1]
        query_part = path_parts[-1]
        
        grouped_output_dir = os.path.join(output_dir, scale, *folder_parts)
        os.makedirs(grouped_output_dir, exist_ok=True)

        fig, ax = plt.subplots(figsize=(12, 6))

        # Get unique components and their corresponding durations for the boxplot
        components = sorted(group['TOOL_NAME'].unique(), key=lambda x: (
            x.startswith('jena'), x.startswith('conver-g-flat'), x.startswith('conver-g-condensed')), reverse=True)
        data_to_plot = [group[group['TOOL_NAME'] == comp]
                        ['TIME_MS'] for comp in components]

        # Create the boxplot
        bp = ax.boxplot(data_to_plot, patch_artist=True,
                        tick_labels=components)  # Added labels

        # Add colors to boxes for better distinction
        for patch, comp in zip(bp['boxes'], components):
            if comp.startswith('jena'):
                patch.set_facecolor('purple')
            elif comp.startswith('conver-g-flat'):
                patch.set_facecolor('orange')
            else:
                patch.set_facecolor('green')

        # Add median lines color
        for median in bp['medians']:
            median.set(color='red', linewidth=2)

        # Improve layout and labels
        title_str = ", ".join([f"{col}={val}" for col, val in zip(available_cols, name)])
        ax.set_title(f'Duration Distribution\n{title_str}')
        ax.set_ylabel('Duration (ms)')
        ax.set_xlabel('Component')

        # Set y-axis scale if requested
        if scale == "log":
            ax.set_yscale("log")
            ax.set_ylabel('Duration Log(ms)')

        # ax.tick_params(axis='x', rotation=45) # Rotate x-axis labels if they overlap
        ax.grid(True, linestyle='--', alpha=0.6)  # Add grid lines

        # --- Create a safe filename for the plot ---
        safe_query = sanitize_filename(query_part)
        filepath = os.path.join(
            grouped_output_dir, f"whisker_duration_{safe_query}.png")

        plt.savefig(filepath, dpi=300)
        plt.close(fig)


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

def check_shapiro_wilk_test(data: list, warmup: int, query_type: str, output_folder: str):
    import pandas as pd
    from scipy import stats
    
    print("The Shapiro-Wilk test is a statistical test used to determine whether a sample comes from a normally distributed population.")   
    print("This means that the data does not significantly deviate from a normal distribution.") 

    df = pd.DataFrame(data)
    
    results = []
    results_without_query = []
    
    # Remove warmup tries
    df = df[df['RUN_ID'] > warmup]
    
    # Grouping for "with query"
    group_cols_with_query = ['POLICY', 'GRANULARITY', 'QUERY', 'TOOL_NAME']
    available_cols_with_query = [col for col in group_cols_with_query if col in df.columns]
    
    grouped = df.groupby(available_cols_with_query)

    # Compute mean duration by RUN_ID and config (averaging over queries for "without query" case)
    mean_group_cols = ['RUN_ID', 'TOOL', 'POLICY', 'GRANULARITY', 'TOOL_NAME']
    available_mean_cols = [col for col in mean_group_cols if col in df.columns]
    
    mean_df = df.groupby(available_mean_cols, as_index=False)['TIME_MS'].mean()
    
    # Now group by config + tool for the test
    group_cols_without_query = ['POLICY', 'GRANULARITY', 'TOOL_NAME']
    available_cols_without_query = [col for col in group_cols_without_query if col in mean_df.columns]
    
    grouped_without_query = mean_df.groupby(available_cols_without_query)
    
    for name, group in grouped_without_query:
        if len(group) < 2:
            continue
        # Perform Shapiro-Wilk test for normality
        grouped_data = group['TIME_MS']
        stat, p_value = stats.shapiro(grouped_data)
        alpha = 0.05
        if p_value > alpha:
            pass # print(f"Data for {name} is normally distributed (p={p_value:.3f})")

        res = {
            'W_STATISTIC': stat,
            'P_VALUE': p_value,
            'NORMALLY_DISTRIBUTED': str(p_value > alpha),
            'MEAN': grouped_data.mean(),
            'MEDIAN': grouped_data.median(),
            '75TH_PERC': grouped_data.quantile(0.75),
            '95TH_PERC': grouped_data.quantile(0.95),
        }
        # Add config keys
        if isinstance(name, tuple):
            for i, col in enumerate(available_cols_without_query):
                res[col] = name[i]
        else:
            res[available_cols_without_query[0]] = name
            
        results_without_query.append(res)
        
    with open(os.path.join(output_folder, f'shapiro_wilk_test_results_without_query_{query_type}.json'), 'w') as f:
        json.dump(results_without_query, f, indent=4)

    for name, group in grouped:
        if len(group) < 2:
            continue
        # Perform Shapiro-Wilk test for normality
        grouped_data = group['TIME_MS']
        stat, p_value = stats.shapiro(grouped_data)
        alpha = 0.05
        if p_value > alpha:
            pass # print(f"Data for {name} is normally distributed (p={p_value:.3f})")
            
        res = {
            'W_STATISTIC': stat,
            'P_VALUE': p_value,
            'NORMALLY_DISTRIBUTED': str(p_value > alpha),
            'MEAN': grouped_data.mean(),
            'MEDIAN': grouped_data.median(),
            '75TH_PERC': grouped_data.quantile(0.75),
            '95TH_PERC': grouped_data.quantile(0.95),
        }
        if isinstance(name, tuple):
            for i, col in enumerate(available_cols_with_query):
                res[col] = name[i]
        else:
            res[available_cols_with_query[0]] = name
            
        results.append(res)
        
    # save results to a json file
    with open(os.path.join(output_folder, f'shapiro_wilk_test_results_{query_type}.json'), 'w') as f:
        json.dump(results, f, indent=4)
        
    return results, results_without_query

def check_Mann_Whitney_U_test(data: list, warmup: int, query_type: str, output_folder: str):
    import pandas as pd
    from scipy import stats
    from itertools import combinations
    
    print("Mann-Whitney U test is a non-parametric test used to determine whether there is a significant difference between the distributions of two independent samples.")
    print("No significant difference between comp1 and comp2 for the given query, meaning the statistical test did not find strong evidence that the two are different.")
    print("Significant difference between comp1 and comp2 for the given query, meaning the statistical test found strong evidence that the two are different.")

    df = pd.DataFrame(data)
    
    results = []
    results_without_query = []
    
    # Remove warmup tries
    df = df[df['RUN_ID'] > warmup]
    
    # Without query
    mean_group_cols = ['RUN_ID', 'TOOL', 'POLICY', 'GRANULARITY', 'TOOL_NAME']
    available_mean_cols = [col for col in mean_group_cols if col in df.columns]
    mean_df = df.groupby(available_mean_cols, as_index=False)['TIME_MS'].mean()
    
    group_cols_without_query = ['POLICY', 'GRANULARITY', 'TOOL_NAME']
    available_cols_without_query = [col for col in group_cols_without_query if col in mean_df.columns]
    
    grouped_without_query = mean_df.groupby(available_cols_without_query)
    
    for name, group in grouped_without_query:
        components = group['TOOL_NAME'].unique()
        if len(components) < 2:
            continue
    
        for comp1, comp2 in combinations(components, 2):
            data1 = group[group['TOOL_NAME'] == comp1]['TIME_MS']
            data2 = group[group['TOOL_NAME'] == comp2]['TIME_MS']
            if len(data1) < 2 or len(data2) < 2:
                continue
            stat, p_value = stats.mannwhitneyu(data1, data2, alternative='two-sided')
            
            alpha = 0.05
            if p_value <= alpha:
                pass # print(f"Significant difference between {comp1} and {comp2} for {name} (p={p_value:.3f})")
                
            res = {
                'COMPONENT_1': comp1,
                'COMPONENT_2': comp2,
                'U_STATISTIC': stat,
                'P_VALUE': p_value,
                'SIGNIFICANT': str(p_value <= alpha)
            }
            if isinstance(name, tuple):
                for i, col in enumerate(available_cols_without_query):
                    res[col] = name[i]
            else:
                res[available_cols_without_query[0]] = name
                
            results_without_query.append(res)
            
    with open(os.path.join(output_folder, f'mann_whitney_u_test_results_without_query_{query_type}.json'), 'w') as f:
        json.dump(results_without_query, f, indent=4)

    # With query
    group_cols_with_query = ['POLICY', 'GRANULARITY', 'QUERY']
    available_cols_with_query = [col for col in group_cols_with_query if col in df.columns]
    
    grouped = df.groupby(available_cols_with_query)

    for name, group in grouped:
        components = group['TOOL_NAME'].unique()
        if len(components) < 2:
            continue
        
        for comp1, comp2 in combinations(components, 2):
            data1 = group[group['TOOL_NAME'] == comp1]['TIME_MS']
            data2 = group[group['TOOL_NAME'] == comp2]['TIME_MS']
            if len(data1) < 2 or len(data2) < 2:
                continue
            stat, p_value = stats.mannwhitneyu(data1, data2, alternative='two-sided')
            
            alpha = 0.05
            if p_value <= alpha:
                pass # print(f"Significant difference between {comp1} and {comp2} for {name} (p={p_value:.3f})")
                
            res = {
                'COMPONENT_1': comp1,
                'COMPONENT_2': comp2,
                'U_STATISTIC': stat,
                'P_VALUE': p_value,
                'SIGNIFICANT': str(p_value <= alpha)
            }
            if isinstance(name, tuple):
                for i, col in enumerate(available_cols_with_query):
                    res[col] = name[i]
            else:
                res[available_cols_with_query[0]] = name
                
            results.append(res)

    # save results to a json file
    with open(os.path.join(output_folder, f'mann_whitney_u_test_results_{query_type}.json'), 'w') as f:
        json.dump(results, f, indent=4)

    return results, results_without_query

def create_shapiro_wilk_test_table(results: list, query_type: str, output_folder: str, with_query: bool):
    import pandas as pd

    filename = os.path.join(output_folder, "with_query" if with_query else "without_query", f'shapiro_wilk_test_results_{query_type}.csv')

    df = pd.DataFrame(results)
    
    potential_index_cols = ['POLICY', 'GRANULARITY', 'QUERY']
    index_cols = [col for col in potential_index_cols if col in df.columns]
    
    if not index_cols:
        print("No index columns found for pivot table.")
        return filename
    
    # Create a pivot table as before
    df = df.pivot_table(index=index_cols,
                        columns='TOOL_NAME',
                        values=['MEDIAN', '75TH_PERC', '95TH_PERC'],
                        aggfunc='first')

    # Prepare multi-level columns: first row is component, second row is statistic
    df.columns = pd.MultiIndex.from_tuples([(comp, stat) for stat, comp in df.columns])

    # Sort columns by component_name (first level)
    df = df.sort_index(axis=1, level=0)

    # Reset index for saving to CSV
    df.reset_index(inplace=True)

    # Save with multi-level columns
    df.to_csv(filename, index=False, index_label=index_cols, header=True, float_format='%.2f')
    
    print("Shapiro-Wilk test results saved to ", filename)
    
    return filename

def highlight_and_bold_converg(row):
    import pandas as pd
    
    # Find all 'conver-g-condensed' columns
    converg_condensed_cols = [col for col in row.index if col[0].startswith('conver-g-condensed')]
    # Find all 'conver-g-flat' columns
    converg_flat_cols = [col for col in row.index if col[0].startswith('conver-g-flat')]
    # Find all 'jena' columns
    jena_cols = [col for col in row.index if col[0].startswith('jena')]
    # Find all other component columns
    not_converg_condensed_cols = [col for col in row.index if not col[0].startswith('conver-g-condensed')]
    
    valid_rows = []

    styles = [''] * len(row)
    for stat in ['MEDIAN', '75TH_PERC', '95TH_PERC']:
        converg_condensed_stat_cols = [col for col in converg_condensed_cols if col[1] == stat]
        converg_flat_stat_cols = [col for col in converg_flat_cols if col[1] == stat]
        jena_stat_cols = [col for col in jena_cols if col[1] == stat]
        other_stat_cols = [col for col in not_converg_condensed_cols if col[1] == stat]
        
        for j_col in jena_stat_cols:
            other_vals = [row[o_col] for o_col in other_stat_cols if pd.notnull(row[o_col])]
            idx = row.index.get_loc(j_col)
            styles[idx] = 'background-color: #E6E6FA' # Light purple
        
        for q_col in converg_flat_stat_cols:
            other_vals = [row[o_col] for o_col in other_stat_cols if pd.notnull(row[o_col])]
            idx = row.index.get_loc(q_col)
            styles[idx] = 'background-color: #FFD580' # Light orange
        
        for q_col in converg_condensed_stat_cols:
            q_val = row[q_col]
            idx = row.index.get_loc(q_col)
            styles[idx] = 'background-color: #90ee90' # Light green
            if q_col[1] == 'MEDIAN':
                other_vals = [row[o_col] for o_col in other_stat_cols if pd.notnull(row[o_col])]
                if all(q_val < o_val for o_val in other_vals if pd.notnull(o_val)):
                    valid_rows.append(row.name)
                    
    # Bold the lines where conver-g-condensed is the best in MEDIAN
    # Make the row more visible
    for i in valid_rows:
        for j in range(len(styles)):
            if styles[j] != '':
                styles[j] += '; font-weight: bold; border: 2px solid black'
            else:
                styles[j] = 'font-weight: bold; border: 2px solid black'

    return styles

def highlight_each_component(row):
    import pandas as pd
    
     # Find all 'conver-g-condensed' columns
    converg_condensed_cols = [col for col in row.index if col[0].startswith('conver-g-condensed')]
    # Find all 'conver-g-flat' columns
    converg_flat_cols = [col for col in row.index if col[0].startswith('conver-g-flat')]
    # Find all 'jena' columns
    jena_cols = [col for col in row.index if col[0].startswith('jena')]
    # Find all other component columns
    def get_other_component_cols(component_prefix):
        return [col for col in row.index if not col[0].startswith(component_prefix)]
    
    def get_other_stat_cols(component_prefix):
        return [col for col in get_other_component_cols(component_prefix) if col[1] == stat]
    
    rows_jena_better = []
    rows_converg_flat_better = []
    rows_converg_condensed_better = []
    
    styles = [''] * len(row)
    for stat in ['MEDIAN', '75TH_PERC', '95TH_PERC']:
        converg_condensed_stat_cols = [col for col in converg_condensed_cols if col[1] == stat]
        converg_flat_stat_cols = [col for col in converg_flat_cols if col[1] == stat]
        jena_stat_cols = [col for col in jena_cols if col[1] == stat]
        
        for j_col in jena_stat_cols:
            j_val = row[j_col]
            other_vals = [row[o_col] for o_col in get_other_stat_cols('jena') if pd.notnull(row[o_col])]
            if j_col[1] == 'MEDIAN':
                if all(j_val < o_val for o_val in other_vals if pd.notnull(o_val)):
                    rows_jena_better.append(row.name)
        
        for q_col in converg_flat_stat_cols:
            q_val = row[q_col]
            other_vals = [row[o_col] for o_col in get_other_stat_cols('conver-g-flat') if pd.notnull(row[o_col])]
            if q_col[1] == 'MEDIAN':
                if all(q_val < o_val for o_val in other_vals if pd.notnull(o_val)):
                    rows_converg_flat_better.append(row.name)
        
        for q_col in converg_condensed_stat_cols:
            q_val = row[q_col]
            other_vals = [row[o_col] for o_col in get_other_stat_cols('conver-g-condensed') if pd.notnull(row[o_col])]
            if q_col[1] == 'MEDIAN':
                if all(q_val < o_val for o_val in other_vals if pd.notnull(o_val)):
                    rows_converg_condensed_better.append(row.name)
                
    for i in rows_jena_better:
        for j in range(len(styles)):
            styles[j] = 'background-color: #E6E6FA;' # Light purple
                
    for i in rows_converg_flat_better:
        for j in range(len(styles)):
            styles[j] = 'background-color: #FFD580;' # Light orange
                
    for i in rows_converg_condensed_better:
        for j in range(len(styles)):
            styles[j] = 'background-color: #90ee90;' # Light green
                
    return styles

def highlight_converg_csv(filename: str):
    import pandas as pd

    df = pd.read_csv(filename, header=[0, 1])
    styled_df = df.style.apply(highlight_and_bold_converg, axis=1).format(precision=2)

    styled_filename = filename.replace('.csv', '_highlighted.html')
    styled_df.to_html(styled_filename)

    print(f"Highlighted table saved to {styled_filename}")


def highlight_each_component_csv(filename: str):
    import pandas as pd
    
    df = pd.read_csv(filename, header=[0, 1])
    styled_df = df.style.apply(highlight_each_component, axis=1).format(precision=2)
    styled_filename = filename.replace('.csv', '_highlighted_each_component.html')
    styled_df.to_html(styled_filename)
    print(f"Highlighted table saved to {styled_filename}")


def create_statistical_test_tables(filtered_data, query_type, output_folder, warmup):
    print("------------------- P-Value Test (Shapiro-Wilk) -------------------")
    shapiro_wilk_test_results, shapiro_wilk_test_results_without_query = check_shapiro_wilk_test(data=filtered_data, warmup=warmup, query_type=query_type, output_folder=output_folder)
    print("------------------- Mann-Whitney U Test -------------------")
    check_Mann_Whitney_U_test(data=filtered_data, warmup=warmup, query_type=query_type, output_folder=output_folder)

    print("Creating statistical test tables in CSV format.")
    filename = create_shapiro_wilk_test_table(shapiro_wilk_test_results, query_type=query_type, output_folder=output_folder, with_query=True)
    filename_without_query = create_shapiro_wilk_test_table(shapiro_wilk_test_results_without_query, query_type=query_type, output_folder=output_folder, with_query=False)

    for f in [filename, filename_without_query]:
        highlight_converg_csv(f)
        highlight_each_component_csv(f)


if __name__ == "__main__":
    # Afficher les informations extraites
    min_repeat = int(os.getenv("COUNT_REPEAT", 200))
    warmup = int(os.getenv("WARMUP", 50))
    mode = os.getenv("MODE", "stats")

    csv_file_path = os.getenv("CSV_FILE_PATH")
    if not csv_file_path:
        raise EnvironmentError(
            "CSV_FILE_PATH environment variable is not set.")
        
    queries_info = os.getenv("QUERIES_CONFIGURATION", "time-csv-to-plots/queries_configuration.json")

    print(f"CSV file path: {csv_file_path}")
    print(f"Minimum repeat: {min_repeat}")

    csv_data = extract_csv_info(csv_file_path, queries_info, min_repeat)
    
    main_output_folder = "results"
    output_folders = [os.path.join(main_output_folder, "without_query"), os.path.join(main_output_folder, "with_query")]

    for folder in output_folders:
        os.makedirs(folder, exist_ok=True)

    store_data_to_json(data=csv_data, file_path="csv_data.json")

    if mode == "plots" or mode == "all":
        for scale in ["linear", "log"]:
            whisker_duration_per_component_query_config(data=csv_data, scale=scale, limit=50)
        
    if mode == "stats" or mode == "all":
        for query_type in ["aggregative", "non-aggregative"]:
            if query_type == "non-aggregative":
                print(f"Processing query type: {query_type}")
                filtered_csv_data = [entry for entry in csv_data if not (entry['AGGREGATIVE'])]
                create_statistical_test_tables(filtered_csv_data, query_type, main_output_folder, warmup)
            else:
                print(f"Processing query type: {query_type}")
                filtered_csv_data = [entry for entry in csv_data if (entry['AGGREGATIVE'])]
                create_statistical_test_tables(filtered_csv_data, query_type, main_output_folder, warmup)
