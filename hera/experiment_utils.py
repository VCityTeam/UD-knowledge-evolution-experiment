def create_service_manifest(metadata_name: str, cleanup: str, selector_name:str, port: int, target_port: int) -> str:
    """
    Creates a Kubernetes service manifest.

    This method creates a Kubernetes service manifest with the specified metadata name, cleanup label,
    selector name, port, and target port. The manifest is returned as a string.

    Args:
        metadata_name (str): The name of the service metadata.
        cleanup (str): The cleanup label.
        selector_name (str): The selector name.
        port (int): The service port.
        target_port (int): The target port.
    """
    return ("apiVersion: v1\n"
            "kind: Service\n"
            "metadata:\n"
            f"   name: {metadata_name}\n"
            "   labels:\n"
            f"       cleanup: '{cleanup}'\n"
            "spec:\n"
            "   selector:\n"
            f"       app: {selector_name}\n"
            "   type: ClusterIP\n"
            "   ports:\n"
            f"   - port: {port}\n"
            f"     targetPort: {target_port}\n")

def create_cleanup_config(version: str, product: str, step: str) -> str:
    return f"v-{version}-p-{product}-s-{step}"

def create_volume_manifest(metadata_name: str, accessModes: str, storage: str) -> str:
    return ("apiVersion: v1\n"
                "kind: PersistentVolumeClaim\n"
                "metadata:\n"
                f"   generateName: {metadata_name}\n"
                "spec:\n"
                "   accessModes:\n"
                f"       - {accessModes}\n"
                "   resources:\n"
                "       requests:\n"
                f"          storage: {storage}\n")