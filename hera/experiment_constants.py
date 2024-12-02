import types
import os

constants = types.SimpleNamespace(
    postgres = "postgres@sha256:4ec37d2a07a0067f176fdcc9d4bb633a5724d2cc4f892c7a2046d054bb6939e5",
    blazegraph = "vcity/blazegraph-cors@sha256:c6f9556ca53ff01304557e349d2f10b3e121dae7230426f4c64fa42b2cbaf805",
    quader = "vcity/quads-loader@sha256:70ee2ffd4a304727425fe59040b994d98c14fea9186aa4cc120145182e0bf331",
    quaque = "vcity/quads-query@sha256:74b337f53386462cd5d37b25d6d780a784398db9e9928b7df52584f974a307a6",
    bsbm = "vcity/bsbm@sha256:34665c65bbb2bfbf4348e19a23de9c51a0eb8fafc6d90802c2eda3522f9c420c",
    quads_transformer = "vcity/quads-creator@sha256:351e87bbd951cdeac03af5f8b0057d0fd5fd382fec49adb3ad769edddc1c4e5a", 
    postgres_username = os.environ.get('POSTGRES_USER', "postgres"),
    postgres_password = os.environ.get('POSTGRES_PASSWORD', "password"),
    ubuntu = "ubuntu@sha256:c62f1babc85f8756f395e6aabda682acd7c58a1b0c3bea250713cd0184a93efa",
    python_requests = "xr09/python-requests@sha256:61a5289993bbbfbe4ab3299428855b83c490aeb277895c2bb6f16ab5f0f74abd",
    quads_querier = "harbor.pagoda.os.univ-lyon1.fr/ud-evolution/quads-querier:v1.2.0"
)