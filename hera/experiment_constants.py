import types
import os

constants = types.SimpleNamespace(
    postgres = "postgres@sha256:4ec37d2a07a0067f176fdcc9d4bb633a5724d2cc4f892c7a2046d054bb6939e5",
    blazegraph = "vcity/blazegraph-cors@sha256:c6f9556ca53ff01304557e349d2f10b3e121dae7230426f4c64fa42b2cbaf805",
    quader = "vcity/quads-loader:ql-v3.0.0",
    quaque = "vcity/quads-query:qq-v3.0.0",
    bsbm = "vcity/bsbm@sha256:7d16cee08c88731d575c1634345f23e49007c19643b170c51f1d8b49eb722caf",
    quads_transformer = "vcity/quads-creator@sha256:351e87bbd951cdeac03af5f8b0057d0fd5fd382fec49adb3ad769edddc1c4e5a", 
    postgres_username = os.environ.get('POSTGRES_USER', "postgres"),
    postgres_password = os.environ.get('POSTGRES_PASSWORD', "password"),
    ubuntu = "ubuntu@sha256:c62f1babc85f8756f395e6aabda682acd7c58a1b0c3bea250713cd0184a93efa",
    python_requests = "xr09/python-requests@sha256:61a5289993bbbfbe4ab3299428855b83c490aeb277895c2bb6f16ab5f0f74abd",
    quads_querier = "harbor.pagoda.liris.cnrs.fr/ud-evolution/quads-querier:v1.2.0",
    new_quads_querier = "harbor.pagoda.liris.cnrs.fr/ud-evolution/quads-querier:v1.4.0",
    get_workflow_logs = "harbor.pagoda.liris.cnrs.fr/ud-evolution/get-workflow-logs:v1.1.0",
    converg_space = "harbor.pagoda.liris.cnrs.fr/ud-evolution/converg-space:v1.0.0",
    log_to_plots = "harbor.pagoda.liris.cnrs.fr/ud-evolution/log-to-plots:v1.2.0",
    space_logs_to_plots = "harbor.pagoda.liris.cnrs.fr/ud-evolution/space-logs-to-plots:v1.2.0",
    repeat = 200,
    cpu_limit = 2,
    memory_request = "4",
    memory_limit = "8",
    timeout = "0",
    ostrich = "rdfostrich/ostrich:latest",
    jena = "stain/jena-fuseki:5.1.0"
)