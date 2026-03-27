import json
import sys
import hashlib
import re
from rdflib import Graph, Literal, Namespace
from rdflib.namespace import RDF, XSD

# =============================================================================
# Namespace definitions — strictly matching BioFlow-Ontology.ttl
# =============================================================================
BIOSCHEMAS = Namespace("https://bioschemas.org/")
EDAM = Namespace("http://edamontology.org/")
OWL = Namespace("http://www.w3.org/2002/07/owl#")
PPLAN = Namespace("http://purl.org/net/p-plan#")
PROV = Namespace("http://www.w3.org/ns/prov#")
SCHEMA = Namespace("https://schema.org/")
SF = Namespace("http://www.sharefair.fr/workflow-schema#")
WFRUN = Namespace("https://w3id.org/ro/terms/workflow-run#")
EX = Namespace("http://example.org/execution/")


def get_canonical_id(text):
    """Generate a URI-safe identifier from a text string.

    If the text contains special characters (common in Argo node names which
    include JSON-encoded parameters), it creates a deterministic ID by
    combining a sanitized prefix with an MD5 hash suffix.
    """
    if not text:
        return "unknown"
    if re.search(r'[^a-zA-Z0-9_\-]', text):
        prefix = text.split('(')[0]
        prefix = re.sub(r'[^a-zA-Z0-9_\-]', '_', prefix)
        hashed = hashlib.md5(text.encode('utf-8')).hexdigest()[:8]
        return f"{prefix}_{hashed}"
    return text


def get_template_name(node_info):
    """Extract the template name from an Argo node.

    Argo nodes reference their template definition in two ways:
    - templateName: a direct string (for local templates)
    - templateRef: an object with 'name' and 'template' fields (for
      WorkflowTemplate references, used by nested sub-workflows)
    Falls back to displayName if neither is present.
    """
    if "templateName" in node_info:
        return node_info["templateName"]
    template_ref = node_info.get("templateRef")
    if template_ref:
        return template_ref.get("template", template_ref.get("name", "unknown-step"))
    return node_info.get("displayName", "unknown-step")


def extract_argo_to_kg(json_filepath, output_filepath="execution_kg.ttl"):
    g = Graph()
    g.bind("bioschemas", BIOSCHEMAS)
    g.bind("edam", EDAM)
    g.bind("p-plan", PPLAN)
    g.bind("prov", PROV)
    g.bind("schema", SCHEMA)
    g.bind("sf", SF)
    g.bind("wfrun", WFRUN)
    g.bind("ex", EX)

    with open(json_filepath, 'r') as f:
        data = json.load(f)

    # =========================================================================
    # 1. WORKFLOW TEMPLATE & AGENT
    #
    # Ontology pattern (description level):
    #   sf:Workflow  --schema:creator-->  prov:Person
    #
    # Ontology pattern (execution level):
    #   sf:WorkflowExecution  --sf:correspondsToWorkflow-->  sf:Workflow
    #   sf:WorkflowExecution  --schema:startTime-->  xsd:dateTime
    #   sf:WorkflowExecution  --schema:endTime-->  xsd:dateTime
    #   sf:WorkflowExecution  --schema:actionStatus-->  schema:ActionStatusType
    # =========================================================================
    metadata = data.get("metadata", {})
    uid = metadata.get("uid", "unknown-uid")
    template_name = metadata.get("labels", {}).get(
        "workflows.argoproj.io/workflow-template", "unknown-template"
    )
    creator_email = metadata.get("labels", {}).get(
        "workflows.argoproj.io/creator-email", "unknown-agent"
    )

    wf_instance_uri = EX[uid]
    wf_template_uri = EX[template_name]
    agent_uri = EX[creator_email.replace("@", "_at_")]

    # sf:Workflow — the abstract workflow template (plan)
    # Subclass of: p-plan:Plan, schema:SoftwareSourceCode, bioschemas:ComputationalWorkflow
    g.add((wf_template_uri, RDF.type, SF.Workflow))
    g.add((wf_template_uri, SCHEMA.name, Literal(template_name)))
    # schema:creator — links the workflow to its author
    # Domain: sf:Workflow, Range: schema:Person | schema:Organization
    g.add((wf_template_uri, SCHEMA.creator, agent_uri))

    # sf:WorkflowExecution — the concrete execution of the workflow
    # Subclass of: prov:Activity, schema:CreateAction
    g.add((wf_instance_uri, RDF.type, SF.WorkflowExecution))
    # sf:correspondsToWorkflow — links execution to its workflow template
    # Domain: sf:WorkflowExecution, Range: sf:Workflow
    g.add((wf_instance_uri, SF.correspondsToWorkflow, wf_template_uri))

    # prov:Person — the agent who triggered the execution
    g.add((agent_uri, RDF.type, PROV.Person))
    g.add((agent_uri, SCHEMA.name, Literal(creator_email)))

    # schema:startTime / schema:endTime — temporal bounds of the workflow execution
    # Domain: sf:WorkflowExecution, Range: xsd:dateTime
    started_at_wf = data.get("status", {}).get("startedAt")
    finished_at_wf = data.get("status", {}).get("finishedAt")
    if started_at_wf:
        g.add((wf_instance_uri, SCHEMA.startTime, Literal(started_at_wf, datatype=XSD.dateTime)))
    if finished_at_wf:
        g.add((wf_instance_uri, SCHEMA.endTime, Literal(finished_at_wf, datatype=XSD.dateTime)))

    # schema:actionStatus — execution outcome
    # Domain: sf:WorkflowExecution, Range: schema:ActionStatusType
    phase_wf = data.get("status", {}).get("phase")
    if phase_wf:
        g.add((wf_instance_uri, SCHEMA.actionStatus, Literal(phase_wf)))

    # =========================================================================
    # 2. WORKFLOW-LEVEL INPUT PARAMETERS
    #
    # Workflow templates define global parameters in spec.arguments.parameters.
    # These are modeled as abstract variables on the template and concrete
    # entities on the execution.
    #
    # Ontology pattern (description level):
    #   sf:Workflow  --sf:inputVariable-->  sf:Variable
    #
    # Ontology pattern (execution level):
    #   sf:WorkflowEntity  --sf:corrspondsToVariable-->  sf:Variable
    #   sf:WorkflowExecution  --sf:used-->  sf:WorkflowEntity
    # =========================================================================
    wf_params = data.get("spec", {}).get("arguments", {}).get("parameters", [])
    for param in wf_params:
        param_name = param.get("name")
        param_val = param.get("value")
        if param_name:
            # sf:Variable — abstract input parameter of the workflow template
            # Subclass of: p-plan:Variable, bioschemas:FormalParameter
            var_uri = EX[f"var_in_{template_name}_{param_name}"]
            g.add((var_uri, RDF.type, SF.Variable))
            g.add((var_uri, SCHEMA.name, Literal(param_name)))
            # sf:inputVariable — links workflow template to its input variable
            # Domain: sf:Workflow | sf:Step, Range: sf:Variable
            g.add((wf_template_uri, SF.inputVariable, var_uri))

            if param_val:
                # sf:WorkflowEntity — concrete data value used during workflow execution
                # Subclass of: sf:Entity (which is subclass of edam:Data, prov:Entity)
                ent_uri = EX[f"{uid}_entity_in_{param_name}"]
                g.add((ent_uri, RDF.type, SF.WorkflowEntity))
                g.add((ent_uri, SCHEMA.value, Literal(param_val)))
                # sf:corrspondsToVariable — links entity to its variable definition
                # Note: typo "corrspondsToVariable" matches the ontology TTL
                # Domain: sf:Entity, Range: sf:Variable
                g.add((ent_uri, SF.corrspondsToVariable, var_uri))
                # sf:used — links execution to consumed data
                # Domain: sf:WorkflowExecution, Range: sf:Entity
                g.add((wf_instance_uri, SF.used, ent_uri))

    # =========================================================================
    # 3. NODE EXTRACTION — First pass: build lookup mappings
    #
    # We need two lookups to correctly resolve:
    #   - node_id -> template URI (for isFollowedBy at the template level)
    #   - node_id -> node type (to filter isFollowedBy to Pod-only edges)
    #   - boundaryID -> parent template URI (for hasPart / isSubPlanOf)
    # =========================================================================
    nodes = data.get("status", {}).get("nodes", {})

    node_to_template = {}  # node_id -> template URI
    node_to_type = {}      # node_id -> Argo node type ("DAG", "Pod", etc.)

    for node_id, node_info in nodes.items():
        raw_template_id = get_template_name(node_info)
        template_id = get_canonical_id(raw_template_id)
        node_to_template[node_id] = EX[template_id]
        node_to_type[node_id] = node_info.get("type")

    # =========================================================================
    # 4. NODE EXTRACTION — Second pass: generate triples
    # =========================================================================
    for node_id, node_info in nodes.items():
        node_uri = EX[node_id]
        node_type = node_info.get("type")
        raw_template_id = get_template_name(node_info)
        template_id = get_canonical_id(raw_template_id)
        step_uri = EX[template_id]

        # schema:name — human-readable name for the template
        g.add((step_uri, SCHEMA.name, Literal(raw_template_id)))

        # ---------------------------------------------------------------------
        # Determine the parent workflow/subworkflow for this node.
        #
        # In Argo, boundaryID points to the parent DAG/TaskGroup execution
        # node. The root DAG node has no boundaryID. We resolve the parent's
        # execution node ID to its template URI for proper nesting.
        # ---------------------------------------------------------------------
        boundary_id = node_info.get("boundaryID")
        if boundary_id and boundary_id in node_to_template:
            parent_template_uri = node_to_template[boundary_id]
        else:
            parent_template_uri = wf_template_uri

        # ---------------------------------------------------------------------
        # Topology: type nodes according to the ontology classes
        # ---------------------------------------------------------------------
        if node_type in ["DAG", "TaskGroup"]:
            # sf:Subworfklow — a sub-workflow called within a parent workflow
            # Note: "Subworfklow" typo is intentional, matching the ontology TTL
            # Subclass of: p-plan:MultiStep, sf:Workflow
            g.add((step_uri, RDF.type, SF.Subworfklow))

            # p-plan:isSubPlanOf — links sub-workflow to its parent workflow
            # Domain: sf:SubWorkflow, Range: sf:Workflow
            g.add((step_uri, PPLAN.isSubPlanOf, parent_template_uri))

            # sf:WorkflowExecution — execution instance for this DAG/TaskGroup
            # Since sf:Subworfklow IS a sf:Workflow, its execution is a WorkflowExecution
            # Subclass of: prov:Activity, schema:CreateAction
            g.add((node_uri, RDF.type, SF.WorkflowExecution))
            # sf:correspondsToWorkflow — links execution to its sub-workflow template
            # Domain: sf:WorkflowExecution, Range: sf:Workflow
            g.add((node_uri, SF.correspondsToWorkflow, step_uri))

        elif node_type == "Pod":
            # sf:Step — an atomic executable step within the workflow
            # Subclass of: p-plan:Step, sf:Startable, schema:HowToStep, edam:Operation
            g.add((step_uri, RDF.type, SF.Step))

            # sf:hasPart — links the parent workflow/subworkflow to this step
            # Domain: sf:Workflow, Range: sf:Step
            g.add((parent_template_uri, SF.hasPart, step_uri))

            # sf:StepExecution — execution instance for this step
            # Subclass of: p-plan:Activity, schema:CreateAction
            g.add((node_uri, RDF.type, SF.StepExecution))
            # p-plan:correspondsToStep — links execution to its planned step
            # Domain: sf:StepExecution, Range: sf:Step
            g.add((node_uri, PPLAN.correspondsToStep, step_uri))

        # ---------------------------------------------------------------------
        # Temporal tracking
        # schema:startTime / schema:endTime
        # Domain: sf:StepExecution | sf:WorkflowExecution, Range: xsd:dateTime
        # ---------------------------------------------------------------------
        started_at = node_info.get("startedAt")
        finished_at = node_info.get("finishedAt")
        if started_at:
            g.add((node_uri, SCHEMA.startTime, Literal(started_at, datatype=XSD.dateTime)))
        if finished_at:
            g.add((node_uri, SCHEMA.endTime, Literal(finished_at, datatype=XSD.dateTime)))

        # schema:actionStatus — execution phase/outcome
        # Domain: sf:StepExecution | sf:WorkflowExecution, Range: schema:ActionStatusType
        phase = node_info.get("phase")
        if phase:
            g.add((node_uri, SCHEMA.actionStatus, Literal(phase)))

        # ---------------------------------------------------------------------
        # Resource consumption
        # wfrun:resourceUsage — links execution to resource usage measurements
        # Domain: sf:StepExecution | sf:WorkflowExecution, Range: schema:PropertyValue
        # ---------------------------------------------------------------------
        resources = node_info.get("resourcesDuration", {})
        if resources:
            for res_type, duration in resources.items():
                res_uri = EX[f"{node_id}_resource_{res_type}"]
                g.add((res_uri, RDF.type, SCHEMA.PropertyValue))
                g.add((res_uri, SCHEMA.name, Literal(res_type)))
                g.add((res_uri, SCHEMA.value, Literal(duration)))
                g.add((node_uri, WFRUN.resourceUsage, res_uri))

        # ---------------------------------------------------------------------
        # Input parameters
        #
        # Each input is modeled at two levels:
        #   Description level: sf:Step --sf:inputVariable--> sf:Variable
        #   Execution level:   sf:StepExecution --sf:used--> sf:StepEntity
        #                      sf:StepEntity --sf:corrspondsToVariable--> sf:Variable
        # ---------------------------------------------------------------------
        inputs = node_info.get("inputs", {}).get("parameters", [])
        for param in inputs:
            param_name = param.get("name")
            param_val = param.get("value")
            if param_name and param_val:
                # sf:Variable — abstract input parameter of the step template
                # Subclass of: p-plan:Variable, bioschemas:FormalParameter
                var_uri = EX[f"var_in_{template_id}_{param_name}"]
                g.add((var_uri, RDF.type, SF.Variable))
                g.add((var_uri, SCHEMA.name, Literal(param_name)))
                # sf:inputVariable — links step template to its input variable
                # Domain: sf:Workflow | sf:Step, Range: sf:Variable
                g.add((step_uri, SF.inputVariable, var_uri))

                # sf:StepEntity — concrete data value consumed during step execution
                # Subclass of: sf:Entity (which is subclass of edam:Data, prov:Entity)
                ent_uri = EX[f"{node_id}_entity_in_{param_name}"]
                g.add((ent_uri, RDF.type, SF.StepEntity))
                g.add((ent_uri, SCHEMA.value, Literal(param_val)))
                # sf:corrspondsToVariable — links entity to its variable definition
                # Note: typo "corrspondsToVariable" matches the ontology TTL
                # Domain: sf:Entity, Range: sf:Variable
                g.add((ent_uri, SF.corrspondsToVariable, var_uri))
                # sf:used — links step execution to its consumed data
                # Domain: sf:StepExecution | sf:WorkflowExecution, Range: sf:Entity
                g.add((node_uri, SF.used, ent_uri))

        # ---------------------------------------------------------------------
        # Output parameters & artifacts
        #
        # Each output is modeled at two levels:
        #   Description level: sf:Step --sf:outputVariable--> sf:Variable
        #   Execution level:   sf:StepExecution --sf:generated--> sf:StepEntity
        #                      sf:StepEntity --sf:corrspondsToVariable--> sf:Variable
        # ---------------------------------------------------------------------
        outputs_params = node_info.get("outputs", {}).get("parameters", [])
        outputs_artifacts = node_info.get("outputs", {}).get("artifacts", [])
        for out in outputs_params + outputs_artifacts:
            out_name = out.get("name")
            if out_name:
                # sf:Variable — abstract output defined in the step template
                var_uri = EX[f"var_out_{template_id}_{out_name}"]
                g.add((var_uri, RDF.type, SF.Variable))
                g.add((var_uri, SCHEMA.name, Literal(out_name)))
                # sf:outputVariable — links step template to its output variable
                # Domain: sf:Workflow | sf:Step, Range: sf:Variable
                g.add((step_uri, SF.outputVariable, var_uri))

                # sf:StepEntity — concrete data produced during step execution
                ent_uri = EX[f"{node_id}_entity_out_{out_name}"]
                g.add((ent_uri, RDF.type, SF.StepEntity))
                g.add((ent_uri, SF.corrspondsToVariable, var_uri))
                # sf:generated — links step execution to its produced data
                # Domain: sf:StepExecution | sf:WorkflowExecution, Range: sf:Entity
                g.add((node_uri, SF.generated, ent_uri))

        # ---------------------------------------------------------------------
        # Control flow: sf:isFollowedBy
        # Domain: sf:Startable, Range: sf:Startable (template level)
        #
        # In Argo, "children" of a Pod node are the next nodes in the DAG
        # execution order. We resolve these to template-level URIs since
        # isFollowedBy is a template-level property.
        #
        # We only emit isFollowedBy when BOTH source and target are Pods,
        # because only sf:Step is a subclass of sf:Startable. DAG/TaskGroup
        # nodes map to sf:Subworfklow which is not Startable.
        #
        # For DAG/TaskGroup nodes, "children" represent the entry points of
        # the sub-workflow. Containment is already captured via sf:hasPart
        # and p-plan:isSubPlanOf, so we skip isFollowedBy for them.
        # ---------------------------------------------------------------------
        if node_type == "Pod":
            children = node_info.get("children", [])
            for child_id in children:
                # Only create isFollowedBy between Steps (both must be Pods)
                if child_id in node_to_type and node_to_type[child_id] == "Pod":
                    child_step_uri = node_to_template[child_id]
                    # sf:isFollowedBy — specifies the next step in the workflow
                    # Domain: sf:Startable, Range: sf:Startable
                    g.add((step_uri, SF.isFollowedBy, child_step_uri))

    g.serialize(destination=output_filepath, format="turtle")
    print(f"Knowledge graph generated successfully in {output_filepath}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python create-wf.py <workflow.json> [output.ttl]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "argo_execution_kg.ttl"

    extract_argo_to_kg(input_file, output_file)
