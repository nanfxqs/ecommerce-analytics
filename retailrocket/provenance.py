"""Source-file checksums used by event and reference provenance."""
import hashlib
from pathlib import Path


def file_sha256(path):
    """Hash file bytes incrementally without loading the entire source."""
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()
