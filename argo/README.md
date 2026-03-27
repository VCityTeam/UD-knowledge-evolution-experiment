# Argo Workflow to Knowledge Graph Extractor

This script (`create-wf.py`) is designed to parse an execution JSON of an Argo Workflow and translate its metadata, architecture, and execution details into a Semantic Knowledge Graph. The output is serialized in the RDF Turtle (`.ttl`) format.

## Goal

The main objective of this script is to capture provenance, lifecycle, and structural data of workflows executed via Argo Workflows, mapping them to standard ontologies and vocabularies. 

The generated Knowledge Graph utilizes several semantic namespaces:
*   **ShareFair (SF):** For workflow, subworkflow, step, and variable representation.
*   **PROV-O (PROV):** For tracking agents and creators (provenance).
*   **Schema.org (SCHEMA):** For temporal data (start/end times), resource consumption, and basic property values.
*   **P-PLAN:** For aligning steps and plans.
*   **Workflow-Run (WFRUN):** For tracking computing resource usages.

By creating this graph, workflow executions become queryable and interoperable, facilitating FAIR (Findable, Accessible, Interoperable, Reusable) data principles for computational pipelines.

## Key Features

1. **Workflow & Agent Extraction:** Maps the overall workflow template, its execution instance, and the agent (creator).
2. **Node Topology:** Distinguishes between `DAG`/`TaskGroup` (Subworkflows) and `Pod` (executable steps), maintaining the parent-child relationships.
3. **Execution Tracking:** Records start and finish times for the workflow and individual steps.
4. **Resource Monitoring:** Extracts computational resources consumed during step execution.
5. **Data Flow (Inputs/Outputs):** Differentiates between abstract workflow *variables* (parameters defined in the template) and concrete *entities* (the actual values/artifacts passed during execution).
6. **URI Normalization:** Automatically sanitizes and hashes complex string identifiers to generate valid URIs, preventing RDF serialization errors.

## Prerequisites

Ensure you have Python installed along with the required dependencies. You can install the required packages using the `requirements.txt` file setup for this workspace:

```bash
pip install -r requirements.txt
```

## Usage

The script is executed via the command line and requires at least the path to the Argo Workflow JSON file. You can optionally specify the path for the output `.ttl` file.

**Command Syntax:**
```bash
python create-wf.py <input-workflow.json> [output-knowledge-graph.ttl]
```

**Examples:**

1. **Basic usage (outputs to default `argo_execution_kg.ttl`):**
   ```bash
   python create-wf.py workflow.json
   ```

2. **Specifying a custom output file:**
   ```bash
   python create-wf.py workflow.json my_custom_graph.ttl
   ```

## Output

The script successfully generates a `.ttl` file containing triples that represent the workflow's structure and execution. This file can be imported into triple stores (like GraphDB, Virtuoso, or Blazegraph) or parsed dynamically by SPARQL engines for further analysis.

## References

- El Garb, M., Coquery, E., Duchateau, F., & Lumineau, N. (2025, July). Improving reproducibility in bioinformatics workflows with BioFlow-Model. In Proceedings of the 3rd ACM Conference on Reproducibility and Replicability (pp. 202-207). - https://dl.acm.org/doi/full/10.1145/3736731.3746139