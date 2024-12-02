import re
from parse_arguments import parse_arguments

def extract_log_info(log_file_path):
    # Définir une expression régulière pour correspondre au format du log
    log_pattern = r"\[Measure\]\s*\((.*?)\):\s*(\d+)\s*ns for query:\s*(.*);"
    extracted_data = []

    # Lire le fichier de logs
    with open(log_file_path, 'r') as file:
        for line in file:
            # Chercher les correspondances avec le pattern
            match = re.search(log_pattern, line)
            if match:
                # Extraire PARAM, TIME, et FILE
                param = match.group(1)
                time_ns = int(match.group(2))
                query = match.group(3)
                
                # Ajouter les informations extraites à la liste
                # Compter le nombre d'éléments ayant le même param et file
                count = sum(1 for entry in extracted_data if entry["PARAM"] == param and entry["QUERY"] == query)
                
                extracted_data.append({
                    "PARAM": param,
                    "TIME (ns)": time_ns,
                    "QUERY": query,
                    "COUNT": count + 1  # Ajouter 1 pour inclure l'élément actuel
                })

    return extracted_data


if __name__ == "__main__":
    # Afficher les informations extraites
    args = parse_arguments()

    print(f"Extracting log data from {args.log_file_path}")
    log_data = extract_log_info(args.log_file_path)

    for entry in log_data:
        print(entry)
