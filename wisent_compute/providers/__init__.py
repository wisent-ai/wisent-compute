from .base import Provider

GCP_PROVIDER = "gcp"
AWS_PROVIDER = "aws"
AZURE_PROVIDER = "azure"


def get_provider(name: str) -> Provider:
    if name == GCP_PROVIDER:
        from .gcp import GCPProvider
        return GCPProvider()
    elif name == AWS_PROVIDER:
        from .aws import AWSProvider
        return AWSProvider()
    elif name == AZURE_PROVIDER:
        from .azure import AzureProvider
        return AzureProvider()
    raise ValueError(f"Unknown provider: {name}")
