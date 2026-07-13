from .base import Provider


def get_provider(name: str) -> Provider:
    if name == "gcp":
        from .gcp import GCPProvider
        return GCPProvider()
    elif name == "aws":
        from .aws import AWSProvider
        return AWSProvider()
    elif name == "azure":
        from .azure import AzureProvider
        return AzureProvider()
    raise ValueError(f"Unknown provider: {name}")
