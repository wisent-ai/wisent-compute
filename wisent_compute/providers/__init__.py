from .base import Provider


def get_provider(name: str) -> Provider:
    if name == "gcp":
        from .gcp import GCPProvider
        return GCPProvider()
    elif name == "aws":
        from .aws import AWSProvider
        return AWSProvider()
    raise ValueError(f"Unknown provider: {name}")
